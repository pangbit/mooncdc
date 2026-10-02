///|
priv struct ChHttp {
  endpoint : String
  user : String
  password : String
  timeout_ms : Int
}

///|
// Staged backend entry point, exercised by live tests until Destination integration.
#warnings("-unused_value")
fn ChHttp::new(
  endpoint : String,
  user : String,
  password : String,
  timeout_ms : Int,
) -> ChHttp raise {
  if !(endpoint.has_prefix("http://") || endpoint.has_prefix("https://")) ||
    endpoint.contains("?") ||
    endpoint.contains("#") ||
    endpoint.contains("@") ||
    endpoint.iter().any(ch => ch <= ' ') ||
    timeout_ms <= 0 {
    raise InvalidConfig(
      "ClickHouse endpoint requires HTTP(S) without credentials, query or fragment",
    )
  }
  for value in [user, password] {
    if value.iter().any(ch => ch == '\r' || ch == '\n' || ch == '\u0000') {
      raise InvalidConfig("invalid ClickHouse credential header")
    }
  }
  {
    endpoint: endpoint + (if endpoint.has_suffix("/") { "" } else { "/" }),
    user,
    password,
    timeout_ms,
  }
}

///|
async fn ChHttp::query(
  self : ChHttp,
  sql : String,
  body? : Bytes = b"",
  insert? : Bool = false,
) -> String {
  @async.with_timeout(self.timeout_ms, () => {
    // Waiting for completion prevents a successful HTTP status preceding a
    // server-side execution error. Synchronous inserts acknowledge server work.
    let client = @http.post_stream(
      self.endpoint +
      "?wait_end_of_query=1&async_insert=0&input_format_with_types_use_header=1",
      headers=Map([
        (@http.CaseInsensitiveString("X-ClickHouse-User"), self.user),
        (@http.CaseInsensitiveString("X-ClickHouse-Key"), self.password),
        (
          @http.CaseInsensitiveString("Content-Type"),
          "application/octet-stream",
        ),
      ]),
    )
    defer client.close()
    client.write(sql + "\n")
    if !body.is_empty() {
      client.write(body)
    }
    let response = client.end_request()
    let output = @buffer.Buffer()
    while client.read_some(max_len=8192) is Some(bytes) {
      if output.length() + bytes.length() > 1048576 {
        raise LimitExceeded("ClickHouse response exceeds 1 MiB")
      }
      output.write_bytes(bytes)
    }
    let text = @utf8.decode_lossy(output.to_bytes())
    if response.code < 200 || response.code >= 300 {
      // Server details may echo input data and SQL. Expose the bounded native
      // code, never the body, credentials or endpoint in persisted errors.
      let code = response.headers
        .get(@http.CaseInsensitiveString("X-ClickHouse-Exception-Code"))
        .unwrap_or("unknown")
      let code = if code.length() <= 12 &&
        code.iter().all(ch => ch >= '0' && ch <= '9') {
        code
      } else {
        "unknown"
      }
      let message = "ClickHouse HTTP \{response.code}, exception \{code}"
      if response.code == 408 ||
        response.code == 429 ||
        (insert && code != "16" && code != "117") {
        raise Retryable(message)
      }
      raise Manual(message)
    }
    text
  })
}

///|
// Staged backend entry point, exercised by live tests until Destination integration.
#warnings("-unused_value")
async fn ChHttp::insert(
  self : ChHttp,
  table : String,
  layout : ChLayout,
  rows : ReadOnlyArray[Bytes],
  max_bytes : Int,
  token : String?,
) -> Unit {
  if max_bytes <= 0 {
    raise InvalidConfig("ClickHouse insert byte limit must be positive")
  }
  let mut first = 0
  let mut statement = 0
  while first < rows.length() {
    let out = @buffer.Buffer()
    out.write_bytes(layout.header)
    let mut bytes = 0
    while first < rows.length() && bytes < max_bytes {
      out.write_bytes(rows[first])
      bytes += rows[first].length()
      first += 1
    }
    let settings = match token {
      Some(token) =>
        " SETTINGS insert_deduplication_token=" +
        ch_quote("\{token}-\{statement}", '\'')
      None => ""
    }
    ignore(
      self.query(
        "INSERT INTO " +
        table +
        " (" +
        layout.column_list +
        ")" +
        settings +
        " FORMAT RowBinaryWithNamesAndTypes",
        body=out.to_bytes(),
        insert=true,
      ),
    )
    statement += 1
  }
}
