///|
fn metrics_listener_address(
  listen_address : String?,
  port : Int,
) -> @socket.Addr? raise {
  if port <= 0 || port > 65535 {
    raise InvalidConfig("metrics port must be between 1 and 65535")
  }
  guard listen_address is Some(address) else { return None }
  if address.is_empty() ||
    !address
    .iter()
    .all(c => {
      (c >= '0' && c <= '9') ||
      (c >= 'a' && c <= 'f') ||
      (c >= 'A' && c <= 'F') ||
      c == '.' ||
      c == ':'
    }) {
    raise InvalidConfig(
      "metrics listen_address must be an IPv4 or IPv6 literal",
    )
  }
  let endpoint = if address.contains(":") {
    "[\{address}]:\{port}"
  } else {
    "\{address}:\{port}"
  }
  Some(
    @socket.Addr::parse(endpoint) catch {
      _ =>
        raise InvalidConfig(
          "metrics listen_address must be an IPv4 or IPv6 literal",
        )
    },
  )
}

///|
/// Validate a numeric IPv4 or unbracketed IPv6 metrics address without resolving
/// DNS or creating a listener. The port is configured separately.
pub fn validate_metrics_listen_address(address : String) -> Unit raise {
  ignore(metrics_listener_address(Some(address), 9000))
}

///|
async fn[T] with_metrics_listener(
  listener : @socket.TcpServer,
  render : () -> String raise,
  run : async () -> T,
) -> T {
  defer listener.close()
  @async.with_task_group(group => {
    group.spawn_bg(() => {
      listener.run_forever(
        (tcp, _) => {
          @async.with_timeout(5000, () => {
            let conn = @http.ServerConnection::new(tcp)
            defer conn.close()
            let request = conn.read_request()
            let path = request.path.split("?").next().unwrap_or("")
            let body = if path == "/health" { "OK" } else { render() }
            let bytes = @utf8.encode(body)
            let headers : Map[@http.CaseInsensitiveString, String] = {
              "Content-Type": "text/plain; version=0.0.4; charset=utf-8",
              "Content-Length": bytes.length().to_string(),
              "Connection": "close",
            }
            conn.send_response(200, "OK", extra_headers=headers)
            if request.meth != Head {
              conn.write(bytes)
            }
            conn.end_response()
          })
        },
        allow_failure=true,
        max_connections=128,
      )
    })
    let result = run()
    group.return_immediately(result)
    result
  })
}

///|
/// Serve this control's Prometheus metrics on all paths except /health ("OK").
/// Binds IPv4/IPv6 before run and closes listener/connections on every exit.
/// The endpoint has no worker-control side effects; HEAD omits the response body.
/// Omitting listen_address preserves all-interface IPv4/IPv6 with IPv4 fallback.
/// An explicit numeric address binds only that address, without family fallback.
pub async fn[T] with_metrics_server(
  control : ApplyControl,
  run : async () -> T,
  port? : Int = 9000,
  listen_address? : String,
  pipeline_id? : String,
  destination? : String,
  project? : String,
  stall_timeout_ms? : Int = 300000,
) -> T {
  let address = metrics_listener_address(listen_address, port)
  let render = () => {
    control.prometheus(pipeline_id?, destination?, project?, stall_timeout_ms~)
  }
  ignore(render())
  let listener = match address {
    None => bind_health_listener(port)
    Some(address) =>
      @socket.TcpServer(address, dual_stack=false, reuse_addr=true)
  }
  with_metrics_listener(listener, render, run)
}
