#include <errno.h>
#include <moonbit.h>

/* Read host constants instead of hard-coding Linux/BSD errno numbers. */
MOONBIT_FFI_EXPORT int32_t mooncdc_errno_eafnosupport(void) {
  return EAFNOSUPPORT;
}

MOONBIT_FFI_EXPORT int32_t mooncdc_errno_eprotonosupport(void) {
  return EPROTONOSUPPORT;
}
