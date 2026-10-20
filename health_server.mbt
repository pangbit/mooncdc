///|
extern "c" fn errno_eafnosupport() -> Int = "mooncdc_errno_eafnosupport"

///|
extern "c" fn errno_eprotonosupport() -> Int = "mooncdc_errno_eprotonosupport"

///|
fn health_ipv4_fallback(error : Error) -> Bool {
  match error {
    @os_error.OSError(code, ..) =>
      code == errno_eafnosupport() || code == errno_eprotonosupport()
    _ => false
  }
}

///|
async fn bind_health_listener(port : Int) -> @socket.TcpServer {
  @socket.TcpServer(
    @socket.Addr::parse("[::]:\{port}"),
    dual_stack=true,
    reuse_addr=true,
  ) catch {
    error =>
      if health_ipv4_fallback(error) {
        @socket.TcpServer(
          @socket.Addr::parse("0.0.0.0:\{port}"),
          reuse_addr=true,
        )
      } else {
        raise error
      }
  }
}

///|
fn health_response(
  control : ApplyControl,
  path : String,
  verb : @http.RequestMethod,
  stall_timeout_ms : Int,
) -> (Int, String) raise {
  let path = path.split("?").next().unwrap_or("")
  if path != "/livez" && path != "/readyz" {
    return (404, "")
  }
  if verb != Get && verb != Head {
    return (405, "")
  }
  let health = control.health(stall_timeout_ms~)
  if path == "/livez" {
    if health.live {
      (200, "ok")
    } else {
      (503, "inactive")
    }
  } else if health.ready {
    (200, "ok")
  } else {
    (503, health.status)
  }
}

///|
async fn[T] with_health_listener(
  control : ApplyControl,
  listener : @socket.TcpServer,
  stall_timeout_ms : Int,
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
            let (status, body) = health_response(
              control,
              request.path,
              request.meth,
              stall_timeout_ms,
            )
            let headers : Map[@http.CaseInsensitiveString, String] = {
              "Content-Type": "text/plain; charset=utf-8",
              "Content-Length": body.length().to_string(),
              "Connection": "close",
            }
            if status == 405 {
              headers["Allow"] = "GET,HEAD"
            }
            let reason = match status {
              200 => "OK"
              503 => "Service Unavailable"
              405 => "Method Not Allowed"
              _ => "Not Found"
            }
            conn.send_response(status, reason, extra_headers=headers)
            if request.meth != Head {
              conn.write(body)
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
/// Serve read-only GET/HEAD /livez and /readyz for the lifetime of run. Binds
/// before invoking run, including pipeline initialization. The listener accepts
/// IPv4/IPv6 on all interfaces, falling back only for unsupported IPv6 sockets.
/// Returning, failing or cancelling run closes the listener and its connections.
/// Omit this wrapper to disable HTTP health; it never stops or resumes workers.
pub async fn[T] with_health_server(
  control : ApplyControl,
  run : async () -> T,
  port? : Int = 9001,
  stall_timeout_ms? : Int = 300000,
) -> T {
  if port <= 0 || port > 65535 {
    raise InvalidConfig("health port must be between 1 and 65535")
  }
  ignore(control.health(stall_timeout_ms~))
  let listener = bind_health_listener(port)
  with_health_listener(control, listener, stall_timeout_ms, run)
}
