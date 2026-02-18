from libc.stdint cimport uint64_t


cdef class Chunk:
    def __init__(self, offset: int, length: int, data: memoryview, gear_hash: int):
        self.offset = offset
        self.length = length
        self.data = data
        self.gear_hash = gear_hash

    @staticmethod
    cdef Chunk _cy_create(uint64_t offset, uint64_t length, memoryview data, uint64_t gear_hash):
        cdef Chunk c = Chunk.__new__(Chunk)
        c.offset = offset
        c.length = length
        c.data = data
        c.gear_hash = gear_hash
        return c

    def __repr__(self) -> str:
        return f'<Chunk offset={self.offset} length={self.length} gear_hash={self.gear_hash}>'
