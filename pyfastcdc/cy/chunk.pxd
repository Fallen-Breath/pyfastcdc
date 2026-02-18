from libc.stdint cimport uint64_t

cdef class Chunk:
    cdef readonly uint64_t offset
    cdef readonly uint64_t length
    cdef readonly memoryview data
    cdef readonly uint64_t gear_hash

    @staticmethod
    cdef Chunk _cy_create(uint64_t offset, uint64_t length, memoryview data, uint64_t gear_hash)
