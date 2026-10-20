#include <stdint.h>
#include <moonbit.h>

#if defined(__APPLE__) || defined(__linux__)
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

/* Read-only acceptance probe for an owned socket; no managed pointers cross FFI. */
MOONBIT_FFI_EXPORT int32_t mooncdc_keepalive_value(int32_t fd, int32_t kind) {
  int level = IPPROTO_TCP, option, value = 0;
  switch (kind) {
    case 0: level = SOL_SOCKET; option = SO_KEEPALIVE; break;
#ifdef __APPLE__
    case 1: option = TCP_KEEPALIVE; break;
#else
    case 1: option = TCP_KEEPIDLE; break;
#endif
    case 2: option = TCP_KEEPINTVL; break;
    case 3: option = TCP_KEEPCNT; break;
    default: return -EINVAL;
  }
  socklen_t size = sizeof(value);
  if (getsockopt(fd, level, option, &value, &size) < 0) return -errno;
  return value;
}
#else
MOONBIT_FFI_EXPORT int32_t mooncdc_keepalive_value(int32_t fd, int32_t kind) {
  (void)fd;
  (void)kind;
  return -1;
}
#endif
