#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <locale.h>
#include <stdlib.h>
#include <moonbit.h>
#ifdef __APPLE__
#include <xlocale.h>
#endif

/* All grammar validation happens in MoonBit. Use a private C locale so a host
 * application's LC_NUMERIC cannot change PostgreSQL's decimal separator.
 * strtof performs one rounding to binary32; double-then-float does not. */
MOONBIT_FFI_EXPORT double mooncdc_parse_float(moonbit_bytes_t text,
                                             int32_t single,
                                             int32_t *status) {
  char *end = NULL;
#ifdef _WIN32
  _locale_t locale = _create_locale(LC_NUMERIC, "C");
  if (!locale) { status[0] = 1; return 0; }
  double value = single ? (double)_strtof_l((char *)text, &end, locale)
                        : _strtod_l((char *)text, &end, locale);
  _free_locale(locale);
#else
  locale_t locale = newlocale(LC_NUMERIC_MASK, "C", (locale_t)0);
  if (!locale) { status[0] = 1; return 0; }
  double value = single ? (double)strtof_l((char *)text, &end, locale)
                        : strtod_l((char *)text, &end, locale);
  freelocale(locale);
#endif
  status[0] = end != (char *)text + Moonbit_array_length(text);
  return value;
}
