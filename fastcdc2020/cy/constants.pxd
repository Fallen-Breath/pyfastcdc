from libc.stdint cimport uint64_t

cdef uint64_t[26] MASKS
cdef uint64_t[256] GEAR
cdef uint64_t[256] GEAR_LS
