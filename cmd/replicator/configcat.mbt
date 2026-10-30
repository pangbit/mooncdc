///|
priv struct ConfigCatOptions {
  sdk_key : String
  default_user : String
  base_url : String
  custom_url : Bool
  poll_ms : Int32
  timeout_ms : Int32
}

///|
priv struct ConfigCatCache {
  mut document : Json?
  mut etag : String
  mut refreshed_ms : Int64?
  mut fetch_url : String
}

///|
fn configcat_key_path(key : String) -> String {
  let result = @buffer.Buffer()
  let hex = "0123456789ABCDEF"
  for byte in @utf8.encode(key) {
    let c = byte.to_int()
    if (c >= 65 && c <= 90) ||
      (c >= 97 && c <= 122) ||
      (c >= 48 && c <= 57) ||
      [45, 46, 47, 95, 126].contains(c) {
      result.write_byte(byte)
    } else {
      result.write_byte(b'%')
      result.write_byte(hex[c / 16].to_int().to_byte())
      result.write_byte(hex[c % 16].to_int().to_byte())
    }
  }
  @utf8.decode_lossy(result.to_bytes())
}

///|
fn configcat_valid_key(key : String, custom : Bool) -> Bool {
  if custom &&
    key.has_prefix("configcat-proxy/") &&
    key.length() > "configcat-proxy/".length() {
    return true
  }
  let parts = key.split("/").collect()
  let valid_section = (part : StringView) => {
    @utf8.encode(part.to_owned()).length() == 22
  }
  (parts.length() == 2 && valid_section(parts[0]) && valid_section(parts[1])) ||
  (
    parts.length() == 3 &&
    parts[0] == "configcat-sdk-1" &&
    valid_section(parts[1]) &&
    valid_section(parts[2])
  )
}

///|
fn parse_configcat(fields : Map[String, Json]) -> ConfigCatOptions? raise {
  guard supabase_fields(fields) is Some(config) else { return None }
  guard optional_report_text(config, "configcat_sdk_key", "supabase")
    is Some(key) else {
    for
      field in [
        "configcat_base_url", "configcat_poll_interval_ms", "configcat_http_timeout_ms",
      ] {
      if config.contains(field) {
        raise ConfigError("supabase." + field + ": requires configcat_sdk_key")
      }
    }
    return None
  }
  let custom = optional_report_text(config, "configcat_base_url", "supabase")
  let base = custom.unwrap_or("https://cdn-global.configcat.com")
  report_url(base, "supabase.configcat_base_url")
  if !configcat_valid_key(key, custom is Some(_)) {
    raise ConfigError("supabase.configcat_sdk_key: invalid SDK key")
  }
  let poll_ms = int_field(
    config, "configcat_poll_interval_ms", "supabase", 60000,
  )
  let timeout_ms = int_field(
    config, "configcat_http_timeout_ms", "supabase", 30000,
  )
  if poll_ms <= 0 || timeout_ms <= 0 {
    raise ConfigError(
      "supabase.configcat: poll interval and timeout must be positive",
    )
  }
  Some({
    sdk_key: key,
    default_user: text_field(config, "project_ref", "supabase"),
    base_url: base.trim_end(chars="/").to_owned(),
    custom_url: custom is Some(_),
    poll_ms,
    timeout_ms,
  })
}

///|
fn ConfigCatCache::new(options : ConfigCatOptions) -> ConfigCatCache raise {
  if options.default_user.is_empty() {
    raise ConfigError("supabase.project_ref: invalid ConfigCat default user")
  }
  { document: None, etag: "", refreshed_ms: None, fetch_url: options.base_url, }
}

///|
fn configcat_preferences(document : Json) -> (String?, Int32) raise {
  guard document is Object(fields) && fields.get("f") is Some(Object(_)) else {
    fail("invalid ConfigCat document")
  }
  match fields.get("s") {
    None | Some(Null) | Some(Array(_)) => ()
    _ => fail("invalid ConfigCat segments")
  }
  match fields.get("p") {
    None | Some(Null) => (None, 0)
    Some(Object(preferences)) => {
      let url = match preferences.get("u") {
        None | Some(Null) => None
        Some(String(url)) => {
          report_url(url, "ConfigCat redirect")
          Some(url.trim_end(chars="/").to_owned())
        }
        _ => fail("invalid ConfigCat redirect")
      }
      let redirect = match preferences.get("r") {
        None | Some(Null) => 0
        Some(Number(value, ..)) if value == 0 || value == 1 || value == 2 =>
          value.to_int()
        _ => fail("invalid ConfigCat redirect mode")
      }
      match preferences.get("s") {
        None | Some(Null) | Some(String(_)) => ()
        _ => fail("invalid ConfigCat salt")
      }
      (url, redirect)
    }
    _ => fail("invalid ConfigCat preferences")
  }
}

///|
async fn ConfigCatCache::refresh(
  self : ConfigCatCache,
  options : ConfigCatOptions,
) -> Unit {
  @async.with_timeout(options.timeout_ms, () => {
    for _ in 0..<3 {
      let headers : @http.Headers = {
        "X-ConfigCat-UserAgent": "ConfigCat-MoonCDC/a-0.1.0",
      }
      if !self.etag.is_empty() {
        headers["If-None-Match"] = self.etag
      }
      let url = self.fetch_url +
        "/configuration-files/" +
        configcat_key_path(options.sdk_key) +
        "/config_v6.json"
      let (response, client) = @http.get_stream(url, headers~)
      defer client.close()
      if response.code == 304 {
        if self.document is Some(_) {
          self.refreshed_ms = Some(@async.now())
        }
        return
      }
      if response.code != 200 {
        if (response.code == 403 || response.code == 404) &&
          self.document is Some(_) {
          self.refreshed_ms = Some(@async.now())
        }
        fail("ConfigCat HTTP status")
      }
      let buffer = @buffer.Buffer()
      while client.read_some(max_len=4096) is Some(bytes) {
        if buffer.length() + bytes.length() > 1048576 {
          fail("ConfigCat response limit")
        }
        buffer.write_bytes(bytes)
      }
      let document = @json.parse(@utf8.decode(buffer.to_bytes()))
      let (preferred, redirect) = configcat_preferences(document)
      if !preferred.map_or(false, base => base == self.fetch_url) &&
        !(options.custom_url &&
        (options.sdk_key.has_prefix("configcat-proxy/") || redirect != 2)) {
        if preferred is Some(base) {
          self.fetch_url = base
        }
        if redirect != 0 {
          continue
        }
      }
      self.document = Some(document)
      self.etag = response.headers.get("ETag").unwrap_or("")
      self.refreshed_ms = Some(@async.now())
      return
    }
    fail("ConfigCat redirect loop")
  })
}

///|
async fn with_configcat(
  options : ConfigCatOptions?,
  run : async () -> Unit,
) -> Unit {
  guard options is Some(options) else { return run() }
  let cache = ConfigCatCache::new(options)
  @async.with_task_group(group => {
    group.spawn_bg(no_wait=true, () => {
      while true {
        let started = @async.now()
        let next = started + options.poll_ms.to_int64()
        if cache.refreshed_ms.map_or(true, time => {
            time <= started - (options.poll_ms / 2).to_int64()
          }) {
          cache.refresh(options) catch {
            _ => eprintln("ConfigCat refresh failed")
          }
        }
        // One scoped request at a time; no queued polls after a slow response.
        let remaining = next - @async.now()
        @async.sleep(
          if remaining <= 0 {
            1
          } else if remaining > options.poll_ms.to_int64() {
            options.poll_ms
          } else {
            remaining.to_int()
          },
        )
      }
    })
    run()
  })
}
