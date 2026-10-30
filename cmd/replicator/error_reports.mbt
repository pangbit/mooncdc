///|
priv struct SentryTarget {
  endpoint : String
  auth : String
  secrets : Array[String]
}

///|
priv struct NotificationTarget {
  endpoint : String
  api_key : String
}

///|
priv struct ErrorReports {
  pipeline_id : String
  project : String?
  sentry : SentryTarget?
  notification : NotificationTarget?
  environment : String
  version : String?
  secrets : Array[String]
}

///|
fn report_token(value : String) -> Bool {
  !value.is_empty() &&
  value
  .iter()
  .all(c => {
    (c >= 'a' && c <= 'z') ||
    (c >= 'A' && c <= 'Z') ||
    (c >= '0' && c <= '9') ||
    c == '-' ||
    c == '_' ||
    c == '.' ||
    c == '%' ||
    c == '~'
  })
}

///|
fn report_url(value : String, field : String) -> Unit raise {
  let invalid = ConfigError(field + ": invalid HTTP(S) endpoint")
  if value.length() > 8192 ||
    value.iter().any(c => c <= ' ' || c >= '\u007f') ||
    value.contains("?") ||
    value.contains("#") ||
    value.contains("@") ||
    value.contains("\\") {
    raise invalid
  }
  guard value.find("://") is Some(i) &&
    (value[:i] == "http" || value[:i] == "https") else {
    raise invalid
  }
  let authority = value[i + 3:].split("/").next().unwrap()
  let parts = authority.split(":").collect()
  if parts.length() > 2 ||
    !report_token(parts[0].to_owned()) ||
    parts[0].contains("%") {
    raise invalid
  }
  if parts.length() == 2 {
    let port = @string.parse_int(parts[1]) catch { _ => raise invalid }
    if port <= 0 ||
      port > 65535 ||
      !parts[1].iter().all(c => c >= '0' && c <= '9') {
      raise invalid
    }
  }
  let segments = value[i + 3:].split("/").collect()
  for index in 1..<segments.length() {
    let segment = segments[index]
    if segment == "." ||
      segment == ".." ||
      (!segment.is_empty() && !report_token(segment.to_owned())) {
      raise invalid
    }
  }
}

///|
fn parse_sentry(dsn : String) -> SentryTarget raise {
  let invalid = ConfigError("sentry.dsn: invalid DSN")
  guard dsn.find("://") is Some(scheme) &&
    dsn[scheme + 3:].find("@") is Some(separator) else {
    raise invalid
  }
  let credentials = dsn[scheme + 3:scheme + 3 + separator].split(":").collect()
  if credentials.length() > 2 ||
    !report_token(credentials[0].to_owned()) ||
    credentials.any(value => {
      !value.is_empty() && !report_token(value.to_owned())
    }) {
    raise invalid
  }
  let base = dsn[:scheme + 3].to_owned() +
    dsn[scheme + 4 + separator:].to_owned()
  report_url(base, "sentry.dsn")
  let base = base.trim_end(chars="/").to_owned()
  let segments = base.split("/").collect()
  if segments.length() < 4 || segments.last().unwrap().is_empty() {
    raise invalid
  }
  let project = segments.last().unwrap().to_owned()
  let prefix = segments[:segments.length() - 1].join("/")
  let auth = "Sentry sentry_key=" +
    credentials[0].to_owned() +
    ", sentry_version=7, sentry_client=mooncdc/0.1.0" +
    (if credentials.length() == 2 && !credentials[1].is_empty() {
      ", sentry_secret=" + credentials[1].to_owned()
    } else {
      ""
    })
  {
    endpoint: prefix + "/api/" + project + "/envelope/",
    auth,
    secrets: [dsn] + credentials.map(value => value.to_owned()),
  }
}

///|
fn optional_report_text(
  fields : Map[String, Json],
  key : String,
  path : String,
) -> String? raise {
  match fields.get(key) {
    None | Some(Null) => None
    Some(String(value)) => Some(value)
    _ => raise ConfigError(path + "." + key + ": expected string")
  }
}

///|
fn supabase_fields(fields : Map[String, Json]) -> Map[String, Json]? raise {
  match fields.get("supabase") {
    None | Some(Null) => None
    Some(value) =>
      Some(
        object(value, "supabase", [
          "project_ref", "api_url", "api_key", "configcat_sdk_key", "configcat_base_url",
          "configcat_poll_interval_ms", "configcat_http_timeout_ms",
        ]),
      )
  }
}

///|
fn parse_error_reports(
  fields : Map[String, Json],
  pipeline_id : String,
) -> ErrorReports raise {
  let sentry = match fields.get("sentry") {
    None | Some(Null) => None
    Some(value) =>
      Some(
        parse_sentry(
          text_field(object(value, "sentry", ["dsn"]), "dsn", "sentry"),
        ),
      )
  }
  let (project, notification, api_key, sdk_key) = match
    supabase_fields(fields) {
    None => (None, None, None, None)
    Some(config) => {
      let project = text_field(config, "project_ref", "supabase")
      if project.length() > 1024 ||
        !report_token(project) ||
        project.contains("%") ||
        project == "." ||
        project == ".." {
        raise ConfigError("supabase.project_ref: invalid project reference")
      }
      let sdk_key = optional_report_text(
        config, "configcat_sdk_key", "supabase",
      )
      let key = optional_report_text(config, "api_key", "supabase")
      let target = match optional_report_text(config, "api_url", "supabase") {
        None => None
        Some(url) => {
          report_url(url, "supabase.api_url")
          guard key is Some(key) &&
            !key.trim().is_empty() &&
            key.length() <= 8192 &&
            !key.iter().any(c => c < ' ' || c >= '\u007f') else {
            raise ConfigError(
              "supabase.api_key: nonblank HTTP header required when api_url is configured",
            )
          }
          let base = url.trim_end(chars="/").to_owned()
          Some(NotificationTarget::{
            endpoint: base + "/system/replication/" + project + "/emails",
            api_key: key,
          })
        }
      }
      (Some(project), target, key, sdk_key)
    }
  }
  {
    pipeline_id,
    project,
    sentry,
    notification,
    environment: "prod",
    version: None,
    secrets: api_key.map(key => [key]).unwrap_or([]) +
    sdk_key.map(key => [key, configcat_key_path(key)]).unwrap_or([]) +
    sentry.map(target => target.secrets).unwrap_or([]),
  }
}

///|
fn ErrorReports::redact(self : ErrorReports, message : String) -> String {
  let mut result = message
  for secret in self.secrets {
    if !secret.is_empty() {
      result = result.replace_all(old=secret, new="[redacted]")
    }
  }
  let bytes = @utf8.encode(result)
  if bytes.length() > 16384 {
    @utf8.decode_lossy(bytes[:16384].to_owned()) + " [truncated]"
  } else {
    result
  }
}

///|
fn ErrorReports::notification_body(
  self : ErrorReports,
  message : String,
) -> String {
  let digest = @crypto.bytes_to_hex_string(
    @crypto.sha256(@utf8.encode(message)),
  )
  (
    {
      "template_alias": "etl-pipeline-notification",
      "dedup_key": Json::Json(
        "pipeline-error:" + self.pipeline_id + ":" + digest,
      ),
      "fields": {
        "pipeline_id": Json::Json(self.pipeline_id),
        "notification_title": "Replication pipeline error",
        "notification_message": "Your replication pipeline encountered an error during execution. Review the details below and check the logs for more information.",
        "notification_details_label": "Error details",
        "notification_details": Json::Json(message),
      },
    } : Json).stringify()
}

///|
fn ErrorReports::envelope(
  self : ErrorReports,
  message : String,
  oid : UInt32?,
) -> String raise {
  guard @env.rand(16) is Some(entropy) else {
    fail("report entropy unavailable")
  }
  let event_id = @crypto.bytes_to_hex_string(entropy)
  let tags : Map[String, Json] = {
    "service": "replicator",
    "destination": "clickhouse",
    "pipeline_id": Json::Json(self.pipeline_id),
  }
  if oid is Some(oid) {
    tags["table_id"] = Json::Json(oid.to_string())
  }
  if self.version is Some(version) {
    tags["version"] = Json::Json(version)
  }
  let event : Json = {
    "event_id": Json::Json(event_id),
    "timestamp": Json::Json(@env.now().to_double() / 1000.0),
    "platform": "other",
    "level": "error",
    "environment": Json::Json(self.environment),
    "tags": Json::Json(tags),
    "exception": {
      "values": [{ "type": "ReplicationError", "value": Json::Json(message) }],
    },
  }
  let body = event.stringify()
  ({ "event_id": Json::Json(event_id) } : Json).stringify() +
  "\n" +
  ({ "type": "event", "length": Json::Json(@utf8.encode(body).length()) } : Json).stringify() +
  "\n" +
  body +
  "\n"
}

///|
async fn report_post(
  endpoint : String,
  body : String,
  headers : @http.Headers,
  timeout_ms : Int32,
) -> String {
  if @utf8.encode(body).length() > 1048576 {
    fail("report request limit")
  }
  @async.with_timeout(timeout_ms, () => {
    let client = @http.post_stream(endpoint, headers~)
    defer client.close()
    client.write(body)
    let response = client.end_request()
    let output = @buffer.Buffer()
    while client.read_some(max_len=4096) is Some(bytes) {
      if output.length() + bytes.length() > 65536 {
        fail("report response limit")
      }
      output.write_bytes(bytes)
    }
    if response.code < 200 || response.code >= 300 {
      fail("report HTTP status")
    }
    @utf8.decode_lossy(output.to_bytes())
  })
}

///|
async fn ErrorReports::report(
  self : ErrorReports,
  message : String,
  oid : UInt32?,
  timeout_ms~ : Int32 = 10000,
) -> Unit {
  let message = self.redact(message)
  if self.sentry is Some(target) {
    ignore(
      report_post(
        target.endpoint,
        self.envelope(message, oid),
        {
          "Content-Type": "application/x-sentry-envelope",
          "X-Sentry-Auth": target.auth,
        },
        timeout_ms,
      ),
    ) catch {
      _ => eprintln("Sentry error report failed")
    }
  }
  if self.notification is Some(target) {
    try {
      let reply = report_post(
        target.endpoint,
        self.notification_body(message),
        { "Content-Type": "application/json", "apikey": target.api_key },
        timeout_ms,
      )
      guard @json.parse(reply)
        is { "message": String(_), "deduplicated": True | False, .. } else {
        fail("notification response shape")
      }
    } catch {
      _ => eprintln("Supabase error report failed")
    }
  }
}

///|
priv struct ReportingStore {
  inner : &@cdc.StateStore
  reports : ErrorReports
  known : Map[UInt32, @cdc.TableSyncPhase]
  mut observation_ready : Bool
}

///|
impl @cdc.StateStore for ReportingStore with fn acquire(self, owner) {
  self.inner.acquire(owner)
  self.known.clear()
  self.observation_ready = false
  // Existing terminal states are not a new update. Read/report failures never
  // replace the state store's successful ownership or save result.
  try {
    if @cdc.state_store_status(self.inner) is Some(status) {
      for table in status.tables {
        self.known[table.oid] = table.phase
      }
    }
    self.observation_ready = true
  } catch {
    _ => eprintln("Error report state observation failed")
  }
}

///|
impl @cdc.StateStore for ReportingStore with fn release(self, owner) {
  self.inner.release(owner)
}

///|
impl @cdc.StateStore for ReportingStore with fn load(self, record) {
  self.inner.load(record)
}

///|
impl @cdc.StateStore for ReportingStore with fn save(self, owner, record, bytes) {
  self.inner.save(owner, record, bytes)
  if record == @cdc.Pipeline {
    try {
      if @cdc.state_store_status(self.inner) is Some(status) {
        let terminal : Array[(UInt32, String)] = []
        for table in status.tables {
          if self.observation_ready &&
            table.phase is @cdc.Errored(reason~, retry~, ..) &&
            !(retry is @cdc.TimedRetry(_)) &&
            self.known.get(table.oid) != Some(table.phase) {
            terminal.push((table.oid, reason))
          }
        }
        self.known.clear()
        for table in status.tables {
          self.known[table.oid] = table.phase
        }
        self.observation_ready = true
        for (oid, message) in terminal {
          self.reports.report(message, Some(oid))
        }
      }
    } catch {
      _ => {
        self.observation_ready = false
        eprintln("Error report state observation failed")
      }
    }
  }
}
