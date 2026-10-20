///|
priv suberror ConfigError {
  ConfigError(String)
}

///|
priv struct HealthConfig {
  port : Int
  stall_ms : Int
}

///|
priv struct ReplicatorConfig {
  id : String
  publication : String
  source : @cdc.ConnectionConfig
  state : @cdc.ConnectionConfig
  state_schema : String
  state_table : String
  endpoint : String
  user : String
  password : String
  database : String
  engine : @cdc.ClickHouseEngine
  health : HealthConfig?
  metrics_port : Int?
  metrics_listen_address : String?
  copy_concurrency : Int
  copy_connections_per_table : Int
  batch_rows : Int
  retry_delay_ms : Int
  retries : Int
  copy_policy : @cdc.TableSyncCopy
  source_migrations : Bool
  slot_failover : Bool
  invalidated_slot_behavior : @cdc.InvalidatedSlotBehavior
  memory_backpressure : @cdc.MemoryBackpressureConfig?
  batch : @cdc.BatchConfig?
  memory_refresh_interval_ms : Int
  copy_monitor_interval_ms : Int
  reports : ErrorReports
  configcat : ConfigCatOptions?
}

///|
fn object(
  value : Json,
  path : String,
  keys : Array[String],
) -> Map[String, Json] raise {
  guard value is Object(fields) else {
    raise ConfigError("\{path}: expected object")
  }
  for key, _ in fields {
    if !keys.contains(key) {
      raise ConfigError("\{path}: unsupported field \{key}")
    }
  }
  fields
}

///|
fn text_field(
  fields : Map[String, Json],
  key : String,
  path : String,
  default? : String,
) -> String raise {
  match fields.get(key) {
    Some(String(value)) => value
    None =>
      match default {
        Some(value) => value
        None => raise ConfigError("\{path}.\{key}: required string")
      }
    _ => raise ConfigError("\{path}.\{key}: expected string")
  }
}

///|
fn integer(value : Json, path : String) -> Int raise {
  match value {
    Number(number, ..) if number >= -2147483648.0 &&
      number <= 2147483647.0 &&
      number.trunc() == number => number.to_int()
    String(value) =>
      @string.parse_int(value) catch {
        _ => raise ConfigError("\{path}: expected integer")
      }
    _ => raise ConfigError("\{path}: expected integer")
  }
}

///|
fn int_field(
  fields : Map[String, Json],
  key : String,
  path : String,
  default : Int,
) -> Int raise {
  match fields.get(key) {
    None => default
    Some(value) => integer(value, path + "." + key)
  }
}

///|
fn bool_field(
  fields : Map[String, Json],
  key : String,
  default : Bool,
) -> Bool raise {
  match fields.get(key) {
    None => default
    Some(True) | Some(String("true")) => true
    Some(False) | Some(String("false")) => false
    _ => raise ConfigError("\{key}: expected boolean")
  }
}

///|
fn ratio_field(
  fields : Map[String, Json],
  key : String,
  path : String,
) -> Float raise {
  let invalid = ConfigError("\{path}.\{key}: expected number")
  let value = match fields.get(key) {
    Some(Number(value, ..)) => value
    Some(String(value)) => {
      if !value
        .iter()
        .all(c => {
          (c >= '0' && c <= '9') ||
          c == '+' ||
          c == '-' ||
          c == '.' ||
          c == 'e' ||
          c == 'E'
        }) {
        raise invalid
      }
      @string.parse_double(value) catch {
        _ => raise invalid
      }
    }
    _ => raise invalid
  }
  Float::from_double(value)
}

///|
fn batch_bytes(value : Json) -> UInt64 raise {
  let invalid = ConfigError(
    "batch.max_bytes: expected unsigned decimal integer",
  )
  let text = match value {
    String(text) => text
    Number(number, repr~) =>
      match repr {
        Some(text) => text
        None if number >= 0.0 &&
          number <= 9007199254740991.0 &&
          number.trunc() == number => number.to_int64().to_string()
        _ => raise invalid
      }
    _ => raise invalid
  }
  if text.is_empty() || !text.iter().all(c => c.is_ascii_digit()) {
    raise invalid
  }
  @string.parse_uint64(text, base=10) catch {
    _ => raise invalid
  }
}

///|
fn batch_config(value : Json) -> @cdc.BatchConfig raise {
  let fields = object(value, "batch", [
    "memory_budget_ratio", "max_bytes", "max_fill_ms",
  ])
  let defaults = @cdc.BatchConfig::new()
  @cdc.BatchConfig::new(
    memory_budget_ratio=if fields.contains("memory_budget_ratio") {
      ratio_field(fields, "memory_budget_ratio", "batch")
    } else {
      defaults.memory_budget_ratio
    },
    max_bytes=match fields.get("max_bytes") {
      Some(value) => batch_bytes(value)
      None => defaults.max_bytes
    },
    max_fill_ms=int_field(fields, "max_fill_ms", "batch", defaults.max_fill_ms),
  )
}

///|
fn connection(value : Json, path : String) -> @cdc.ConnectionConfig raise {
  let fields = object(value, path, [
    "host", "hostaddr", "port", "user", "password", "database", "security", "ca_file",
    "timeout_ms", "keepalive",
  ])
  let security = match
    text_field(fields, "security", path, default="verify_full") {
    "verify_full" => @cdc.VerifyFull
    "local_plaintext" => @cdc.LocalPlaintext
    "ca_file" => @cdc.VerifyCAFile(text_field(fields, "ca_file", path))
    _ =>
      raise ConfigError(
        "\{path}.security: expected verify_full, ca_file or local_plaintext",
      )
  }
  if fields.contains("ca_file") && !(security is @cdc.VerifyCAFile(_)) {
    raise ConfigError("\{path}.ca_file: requires ca_file security")
  }
  let hostaddr = match fields.get("hostaddr") {
    None | Some(Null) => None
    Some(String(ip)) => Some(ip)
    _ => raise ConfigError("\{path}.hostaddr: expected string or null")
  }
  let keepalive_path = path + ".keepalive"
  let keepalive = object(
    fields.get("keepalive").unwrap_or(Json::object(Map([]))),
    keepalive_path,
    ["idle_secs", "interval_secs", "retries"],
  )
  @cdc.ConnectionConfig::new(
    host=text_field(fields, "host", path),
    hostaddr?,
    port=int_field(fields, "port", path, 5432),
    user=text_field(fields, "user", path),
    password=text_field(fields, "password", path, default=""),
    database=text_field(fields, "database", path, default="postgres"),
    security~,
    timeout_ms=int_field(fields, "timeout_ms", path, 5000),
    keepalive=@cdc.TcpKeepaliveConfig::new(
      idle_secs=int_field(keepalive, "idle_secs", keepalive_path, 30),
      interval_secs=int_field(keepalive, "interval_secs", keepalive_path, 10),
      retries=int_field(keepalive, "retries", keepalive_path, 3),
    ),
  )
}

///|
fn copy_policy(value : Json) -> @cdc.TableSyncCopy raise {
  let fields = object(value, "table_sync_copy", ["type", "table_ids"])
  let kind = text_field(fields, "type", "table_sync_copy")
  if (kind == "include_tables" || kind == "skip_tables") &&
    !fields.contains("table_ids") {
    raise ConfigError("table_sync_copy.table_ids: required for table selection")
  }
  let ids : Array[UInt] = match fields.get("table_ids") {
    None => []
    Some(Array(ids)) =>
      ids.map(value => {
        match value {
          Number(n, ..) if n >= 0.0 && n <= 4294967295.0 && n.trunc() == n =>
            n.to_uint()
          String(s) =>
            @string.parse_uint(s) catch {
              _ => raise ConfigError("table_sync_copy.table_ids: expected OIDs")
            }
          _ => raise ConfigError("table_sync_copy.table_ids: expected OIDs")
        }
      })
    _ => raise ConfigError("table_sync_copy.table_ids: expected array")
  }
  match kind {
    "include_all_tables" | "skip_all_tables" => {
      if fields.contains("table_ids") {
        raise ConfigError(
          "table_sync_copy.table_ids: only valid for table selection",
        )
      }
      if kind == "include_all_tables" {
        @cdc.IncludeAllTables
      } else {
        @cdc.SkipAllTables
      }
    }
    "include_tables" => @cdc.IncludeTables(ReadOnlyArray::from_array(ids))
    "skip_tables" => @cdc.SkipTables(ReadOnlyArray::from_array(ids))
    _ => raise ConfigError("table_sync_copy.type: unsupported selection")
  }
}

///|
fn parse_config(value : Json) -> ReplicatorConfig raise {
  let fields = object(value, "config", [
    "id", "publication", "source", "state", "destination", "health", "invalidated_slot_behavior",
    "copy_concurrency", "copy_connections_per_table", "batch_rows", "table_retry_delay_ms",
    "max_table_retries", "table_sync_copy", "run_source_migrations", "replication_slot",
    "memory_backpressure", "memory_refresh_interval_ms", "table_sync_monitor_refresh_interval_ms",
    "batch", "metrics", "sentry", "supabase",
  ])
  let source = connection(
    fields.get("source").unwrap_or(Json::null()),
    "source",
  )
  let replication_slot = object(
    fields.get("replication_slot").unwrap_or(Json::object(Map([]))),
    "replication_slot",
    ["failover"],
  )
  let state = object(
    fields.get("state").unwrap_or(Json::object(Map([]))),
    "state",
    ["connection", "schema", "table"],
  )
  let dest_value = fields.get("destination").unwrap_or(Json::null())
  guard dest_value is Object(raw_dest) else {
    raise ConfigError("destination: expected object")
  }
  let kind = text_field(raw_dest, "type", "destination")
  if kind != "clickhouse" {
    raise ConfigError(
      "destination.type: only clickhouse is supported by the core module; other targets require the unpublished extensions module",
    )
  }
  let dest = object(dest_value, "destination", [
    "type", "url", "user", "password", "database", "engine",
  ])
  let engine = match
    text_field(dest, "engine", "destination", default="replacing_merge_tree") {
    "replacing_merge_tree" => @cdc.ReplacingMergeTree
    "merge_tree" => @cdc.MergeTree
    _ => raise ConfigError("destination.engine: unsupported ClickHouse engine")
  }
  let health = match fields.get("health") {
    None | Some(Null) => None
    Some(value) => {
      let fields = object(value, "health", ["port", "stall_timeout_ms"])
      let port = int_field(fields, "port", "health", 9001)
      let stall_ms = int_field(fields, "stall_timeout_ms", "health", 300000)
      if port <= 0 || port > 65535 || stall_ms <= 0 {
        raise ConfigError("health: invalid port or stall timeout")
      }
      Some({ port, stall_ms, })
    }
  }
  let id = text_field(fields, "id", "config")
  let (metrics_port, metrics_listen_address) = match fields.get("metrics") {
    Some(Null) => (None, None)
    value => {
      let metrics = object(value.unwrap_or(Json::object(Map([]))), "metrics", [
        "port", "enabled", "listen_address",
      ])
      let port = int_field(metrics, "port", "metrics", 9000)
      if port <= 0 || port > 65535 {
        raise ConfigError("metrics.port: must be between 1 and 65535")
      }
      let address = match metrics.get("listen_address") {
        None => None
        Some(String(address)) => {
          @cdc.validate_metrics_listen_address(address) catch {
            _ =>
              raise ConfigError(
                "metrics.listen_address: expected an IPv4 or IPv6 literal",
              )
          }
          Some(address)
        }
        _ => raise ConfigError("metrics.listen_address: expected string")
      }
      if bool_field(metrics, "enabled", true) {
        (Some(port), address)
      } else {
        (None, address)
      }
    }
  }
  if metrics_port is Some(port) &&
    health is Some({ port: health_port, .. }) &&
    port == health_port {
    raise ConfigError("metrics and health require distinct ports")
  }
  if id.is_empty() {
    raise ConfigError("id: must not be empty")
  }
  let refresh_interval_ms = int_field(
    fields, "memory_refresh_interval_ms", "config", 100,
  )
  if refresh_interval_ms <= 0 {
    raise ConfigError("memory_refresh_interval_ms: must be positive")
  }
  let copy_monitor_interval_ms = int_field(
    fields, "table_sync_monitor_refresh_interval_ms", "config", 10000,
  )
  if copy_monitor_interval_ms <= 0 {
    raise ConfigError(
      "table_sync_monitor_refresh_interval_ms: must be positive",
    )
  }
  let retry_delay_ms = int_field(
    fields, "table_retry_delay_ms", "config", 10000,
  )
  if retry_delay_ms < 1000 || retry_delay_ms > 86400000 {
    raise ConfigError("table_retry_delay_ms: must be between 1000 and 86400000")
  }
  let memory_backpressure = match fields.get("memory_backpressure") {
    None => Some(@cdc.MemoryBackpressureConfig::new(refresh_interval_ms~))
    Some(Null) => None
    Some(value) => {
      let pressure = object(value, "memory_backpressure", [
        "activate_threshold", "resume_threshold",
      ])
      Some(
        @cdc.MemoryBackpressureConfig::new(
          activate_threshold=ratio_field(
            pressure, "activate_threshold", "memory_backpressure",
          ),
          resume_threshold=ratio_field(
            pressure, "resume_threshold", "memory_backpressure",
          ),
          refresh_interval_ms~,
        ),
      )
    }
  }
  {
    id,
    memory_backpressure,
    memory_refresh_interval_ms: refresh_interval_ms,
    copy_monitor_interval_ms,
    batch: match fields.get("batch") {
      None => Some(@cdc.BatchConfig::new())
      Some(Null) => None
      Some(value) => Some(batch_config(value))
    },
    source_migrations: bool_field(fields, "run_source_migrations", true),
    slot_failover: bool_field(replication_slot, "failover", false),
    publication: text_field(fields, "publication", "config"),
    source,
    state: match state.get("connection") {
      Some(value) => connection(value, "state.connection")
      None => source
    },
    state_schema: text_field(state, "schema", "state", default="public"),
    state_table: text_field(state, "table", "state", default="mooncdc_state"),
    endpoint: text_field(dest, "url", "destination"),
    user: text_field(dest, "user", "destination"),
    password: text_field(dest, "password", "destination", default=""),
    database: text_field(dest, "database", "destination"),
    engine,
    health,
    metrics_port,
    metrics_listen_address,
    reports: parse_error_reports(fields, id),
    configcat: parse_configcat(fields),
    copy_concurrency: int_field(fields, "copy_concurrency", "config", 4),
    copy_connections_per_table: int_field(
      fields, "copy_connections_per_table", "config", 4,
    ),
    invalidated_slot_behavior: match
      text_field(fields, "invalidated_slot_behavior", "config", default="error") {
      "error" => @cdc.RejectInvalidatedSlot
      "recreate" => @cdc.RecreateInvalidatedSlot
      _ =>
        raise ConfigError(
          "config.invalidated_slot_behavior: expected error or recreate",
        )
    },
    batch_rows: int_field(fields, "batch_rows", "config", 256),
    retry_delay_ms,
    retries: int_field(fields, "max_table_retries", "config", 5),
    copy_policy: match fields.get("table_sync_copy") {
      Some(value) => copy_policy(value)
      None => @cdc.IncludeAllTables
    },
  }
}

///|
fn merge_config(base : Json, override_ : Json) -> Json {
  match (base, override_) {
    (Object(base), Object(overrides)) => {
      let merged = base.copy()
      for key, value in overrides {
        merged[key] = merge_config(
          merged.get(key).unwrap_or(Json::null()),
          value,
        )
      }
      Json::object(merged)
    }
    _ => override_
  }
}

///|
fn env_override(
  value : Json,
  path : Array[StringView],
  index : Int,
  replacement : String,
) -> Json raise {
  if index == path.length() {
    return Json::string(replacement)
  }
  let key = path[index].to_owned().to_lower()
  if key.is_empty() {
    raise ConfigError("empty environment override path")
  }
  let fields = match value {
    Object(fields) => fields.copy()
    _ => Map([])
  }
  fields[key] = env_override(
    fields.get(key).unwrap_or(Json::null()),
    path,
    index + 1,
    replacement,
  )
  Json::object(fields)
}

///|
async fn read_config_file(path : String) -> Json {
  let file = @fs.open(path, mode=ReadOnly)
  defer file.close()
  let size = file.size()
  if size < 0 || size > 1048576 {
    raise ConfigError("configuration file exceeds 1 MiB")
  }
  let yaml = path.has_suffix(".yaml") || path.has_suffix(".yml")
  let source = @utf8.decode(file.read_exactly(size.to_int())) catch {
    _ => raise ConfigError("configuration file is not valid UTF-8")
  }
  if yaml {
    @yaml.parse_config(source) catch {
      _ =>
        raise ConfigError(
          "configuration file is not valid YAML or exceeds parsing limits",
        )
    }
  } else {
    @json.parse(source) catch {
      _ => raise ConfigError("configuration file is not valid JSON")
    }
  }
}

///|
async fn find_config_file(dir : String, stem : String) -> String {
  for suffix in ["yaml", "yml", "json"] {
    let path = dir + "/" + stem + "." + suffix
    if @fs.exists(path) && @fs.kind(path) is Regular {
      return path
    }
  }
  raise ConfigError("missing \{stem} configuration file (yaml, yml or json)")
}

///|
async fn load_config(
  path : String?,
  vars : Map[String, String],
) -> ReplicatorConfig {
  let mut value = match path {
    Some(path) => read_config_file(path)
    None => {
      let dir = vars.get("APP_CONFIG_DIR").unwrap_or("configuration")
      let environment = vars.get("APP_ENVIRONMENT").unwrap_or("prod").to_lower()
      if !["dev", "staging", "prod"].contains(environment) {
        raise ConfigError("APP_ENVIRONMENT: expected dev, staging or prod")
      }
      merge_config(
        read_config_file(find_config_file(dir, "base")),
        read_config_file(find_config_file(dir, environment)),
      )
    }
  }
  for key, replacement in vars {
    if key.has_prefix("APP_") &&
      key != "APP_CONFIG_DIR" &&
      key != "APP_ENVIRONMENT" &&
      key != "APP_VERSION" {
      value = env_override(value, key[4:].split("__").collect(), 0, replacement)
    }
  }
  let config = parse_config(value)
  let environment = vars.get("APP_ENVIRONMENT").unwrap_or("prod").to_lower()
  if !["dev", "staging", "prod"].contains(environment) {
    raise ConfigError("APP_ENVIRONMENT: expected dev, staging or prod")
  }
  {
    ..config,
    reports: {
      ..config.reports,
      environment,
      version: vars.get("APP_VERSION"),
    },
  }
}
