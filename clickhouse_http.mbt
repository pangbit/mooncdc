///|
priv struct ChHttp {
  endpoint : String
  user : String
  password : String
  timeout_ms : Int
  connectivity_ms : Int
  schema_ms : Int
  ddl_ms : Int
  insert_ms : Int
  epsilon_ms : Int
}

///|
priv enum ChOperation {
  Connectivity
  SchemaQuery
  Ddl
  InsertRequest
} derive(Eq)

///|
fn ChHttp::new(
  endpoint : String,
  user : String,
  password : String,
  timeout_ms : Int,
  connectivity_ms? : Int = 8000,
  schema_ms? : Int = 16000,
  ddl_ms? : Int = 128000,
  insert_ms? : Int = 256000,
  epsilon_ms? : Int = 4000,
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
  if epsilon_ms < 0 ||
    [connectivity_ms, schema_ms, ddl_ms, insert_ms].any(budget => {
      budget < 0 || budget > 2147483647 - epsilon_ms
    }) {
    raise InvalidConfig("invalid ClickHouse operation timeout budget")
  }
  {
    endpoint: endpoint + (if endpoint.has_suffix("/") { "" } else { "/" }),
    user,
    password,
    timeout_ms,
    connectivity_ms,
    schema_ms,
    ddl_ms,
    insert_ms,
    epsilon_ms,
  }
}

///|
fn ChHttp::server_budget(self : ChHttp, operation : ChOperation) -> Int {
  match operation {
    Connectivity => self.connectivity_ms
    SchemaQuery => self.schema_ms
    Ddl => self.ddl_ms
    InsertRequest => self.insert_ms
  }
}

///|
fn ch_timeout_seconds(milliseconds : Int) -> String {
  // Zero disables several ClickHouse server limits, so floor at one second.
  (milliseconds / 1000).max(1).to_string()
}

///|
fn ChHttp::query_settings(self : ChHttp, operation : ChOperation) -> String {
  let connection = ch_timeout_seconds(self.connectivity_ms)
  let insert = ch_timeout_seconds(self.insert_ms)
  let server = ch_timeout_seconds(self.server_budget(operation))
  "?wait_end_of_query=1&async_insert=0&input_format_with_types_use_header=1" +
  "&connect_timeout=" +
  connection +
  "&http_connection_timeout=" +
  connection +
  "&http_send_timeout=" +
  insert +
  "&http_receive_timeout=" +
  insert +
  (if operation == InsertRequest {
    ""
  } else {
    "&max_execution_time=" + server
  }) +
  (if operation == Ddl { "&lock_acquire_timeout=" + server } else { "" })
}

///|
async fn ChHttp::query(
  self : ChHttp,
  sql : String,
  body? : Bytes = b"",
  insert? : Bool = false,
  operation? : ChOperation = Ddl,
) -> String {
  let operation = if insert { InsertRequest } else { operation }
  let timeout = (self.server_budget(operation) + self.epsilon_ms).min(
    self.timeout_ms,
  )
  @async.with_timeout(timeout, () => {
    // Waiting for completion prevents a successful HTTP status preceding a
    // server-side execution error. Synchronous inserts acknowledge server work.
    let client = @http.post_stream(
      self.endpoint + self.query_settings(operation),
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
