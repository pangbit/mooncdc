#include <stdint.h>
#include <string.h>
#include <moonbit.h>

#ifdef __APPLE__
#include <mach/mach.h>
#include <sys/sysctl.h>
#endif

MOONBIT_FFI_EXPORT int32_t mooncdc_memory_platform(void) {
#ifdef __linux__
  return 1;
#elif defined(__APPLE__)
  return 2;
#else
  return 0;
#endif
}

/* Borrowed 16-byte output, encoded explicitly; no alignment or pointer lifetime
 * assumptions. The Mach send right is released on every path. */
MOONBIT_FFI_EXPORT int32_t mooncdc_host_memory(moonbit_bytes_t out) {
#ifdef __APPLE__
  uint64_t total = 0, used;
  size_t size = sizeof(total);
  int mib[] = {CTL_HW, HW_MEMSIZE};
  if (sysctl(mib, 2, &total, &size, NULL, 0) != 0 || size != sizeof(total))
    return 0;
  mach_port_t host = mach_host_self();
  vm_size_t page_size = 0;
  vm_statistics64_data_t vm;
  mach_msg_type_number_t count = HOST_VM_INFO64_COUNT;
  kern_return_t result = host_page_size(host, &page_size);
  if (result == KERN_SUCCESS)
    result = host_statistics64(host, HOST_VM_INFO64, (host_info64_t)&vm, &count);
  mach_port_deallocate(mach_task_self(), host);
  if (result != KERN_SUCCESS || count < HOST_VM_INFO64_COUNT) return 0;
  used = vm.internal_page_count > vm.purgeable_count
      ? (uint64_t)vm.internal_page_count - vm.purgeable_count : 0;
  /* Counters are 32-bit; their sum and a native page size fit uint64_t. */
  used = (used + vm.wire_count + vm.compressor_page_count) * page_size;
  for (int i = 0; i < 8; ++i) {
    out[i] = (uint8_t)(used >> (i * 8));
    out[8 + i] = (uint8_t)(total >> (i * 8));
  }
  return 1;
#else
  (void)out;
  return 0;
#endif
}
