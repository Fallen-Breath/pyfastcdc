import dataclasses
import io
import mmap
import os
from pathlib import Path
from typing import Optional, Literal, Union, Callable

from libc.stdint cimport uint8_t, uint32_t, uint64_t
from libc.stdlib cimport malloc, free
from libc.string cimport memmove
from fastcdc2020.cy.constants cimport GEAR, GEAR_LS, MASKS

NormalizedChunking = Literal[0, 1, 2, 3]


@dataclasses.dataclass(frozen=True)
class Chunk:
	hash: int  # FastCDC's gear hash
	offset: int
	length: int
	data: memoryview


cdef struct _Config:
	uint32_t avg_size
	uint32_t min_size
	uint32_t max_size
	uint64_t mask_s
	uint64_t mask_l
	uint64_t mask_s_ls
	uint64_t mask_l_ls
	const uint64_t* gear
	const uint64_t* gear_ls


cdef uint64_t MIN_SIZE_LOWER_BOUND = 64
cdef uint64_t AVG_SIZE_LOWER_BOUND = 256
cdef uint64_t MAX_SIZE_LOWER_BOUND = 1024
cdef uint64_t MIN_SIZE_UPPER_BOUND = 1 * 1048576
cdef uint64_t AVG_SIZE_UPPER_BOUND = 4 * 1048576
cdef uint64_t MAX_SIZE_UPPER_BOUND = 16 * 1048576


cdef class FastCDC2020:
	cdef _Config config
	cdef uint64_t* gear_holder
	cdef uint64_t* gear_holder_ls

	def __init__(
			self,
			avg_size: int = 16384,
			min_size: Optional[int] = None,
			max_size: Optional[int] = None,
			*,
			normalized_chunking: NormalizedChunking = 1,
			seed: int = 0,
	):
		if min_size is None:
			min_size = avg_size // 4
		if max_size is None:
			max_size = avg_size * 4
		if not (AVG_SIZE_LOWER_BOUND <= avg_size <= AVG_SIZE_UPPER_BOUND):
			raise ValueError(f'min_size {avg_size} is out of range [{AVG_SIZE_LOWER_BOUND}, {AVG_SIZE_UPPER_BOUND}]')
		if not (MIN_SIZE_LOWER_BOUND <= min_size <= MIN_SIZE_UPPER_BOUND):
			raise ValueError(f'min_size {min_size} is out of range [{MIN_SIZE_LOWER_BOUND}, {MIN_SIZE_UPPER_BOUND}]')
		if not (MAX_SIZE_LOWER_BOUND <= max_size <= MAX_SIZE_UPPER_BOUND):
			raise ValueError(f'min_size {max_size} is out of range [{MAX_SIZE_LOWER_BOUND}, {MAX_SIZE_UPPER_BOUND}]')
		if not (min_size <= avg_size <= max_size):
			raise ValueError(f'avg_size {avg_size} is out of range [{min_size}, {max_size}]')

		self.config.avg_size = avg_size
		self.config.min_size = min_size
		self.config.max_size = max_size

		bits = avg_size.bit_length() - 1
		self.config.mask_s = MASKS[bits + normalized_chunking]
		self.config.mask_l = MASKS[bits - normalized_chunking]
		self.config.mask_s_ls = self.config.mask_s << 1
		self.config.mask_l_ls = self.config.mask_l << 1

		self.config.gear = GEAR
		self.config.gear_ls = GEAR_LS
		self.gear_holder = NULL
		self.gear_holder_ls = NULL
		if seed > 0:
			self.gear_holder = <uint64_t*>malloc(256 * sizeof(uint64_t))
			self.gear_holder_ls = <uint64_t*>malloc(256 * sizeof(uint64_t))
			for i in range(256):
				self.gear_holder[i] = GEAR[i] ^ seed
				self.gear_holder_ls[i] = GEAR_LS[i] ^ seed
			self.config.gear = self.gear_holder
			self.config.gear_ls = self.gear_holder_ls

	def __dealloc__(self):
		if self.gear_holder:
			free(self.gear_holder)
			self.gear_holder = NULL
		if self.gear_holder_ls:
			free(self.gear_holder_ls)
			self.gear_holder_ls = NULL

	def cut_buf(self, buf: Union[bytes, bytearray, memoryview]) -> _BufferChunkSpliter:
		if isinstance(buf, (bytes, bytearray)):
			buf = memoryview(buf)
		return _BufferChunkSpliter(self, buf)

	def cut_file(self, file_path: Union[str, bytes, Path]) -> _FileChunkSpliter:
		file_size = os.path.getsize(file_path)
		return _FileChunkSpliter(self, open(file_path, 'rb'), file_size)

	def cut_stream(self, stream: object) -> _StreamChunkSpliter:
		readinto_func: _ReadintoFunc = getattr(stream, 'readinto', None)
		if readinto_func is None:
			read_func = getattr(stream, 'read', None)
			if read_func is None:
				raise TypeError('stream must be readable')

			def readinto_using_read(dest_buf: memoryview):
				read_buf = read_func(len(dest_buf))
				dest_buf[:len(read_buf)] = read_buf
				return len(read_buf)

			readinto_func = readinto_using_read

		return _StreamChunkSpliter(self, readinto_func)


cdef struct _CutResult:
	uint64_t gear_hash
	uint64_t cut_offset


cdef _CutResult _cut_gear(const _Config* config, const uint8_t* buf, uint64_t buf_len):
	cdef uint64_t remaining = buf_len
	if remaining <= config.min_size:
		return _CutResult(0, remaining)
	cdef uint64_t center = config.avg_size
	if remaining > config.max_size:
		remaining = config.max_size
	elif remaining < center:
		center = remaining

	cdef uint64_t idx = config.min_size // 2
	cdef uint64_t gear_hash = 0
	cdef uint64_t pos = 0

	cdef const uint64_t * gear_ls_ptr = config.gear_ls
	cdef const uint64_t * gear_ptr = config.gear

	while idx < center // 2:
		pos = idx * 2
		gear_hash = (gear_hash << 2) + gear_ls_ptr[buf[pos]]
		if (gear_hash & config.mask_s_ls) == 0:
			return _CutResult(gear_hash, pos)
		gear_hash = gear_hash + gear_ptr[buf[pos + 1]]
		if (gear_hash & config.mask_s) == 0:
			return _CutResult(gear_hash, pos + 1)
		idx += 1

	while idx < remaining // 2:
		pos = idx * 2
		gear_hash = ((gear_hash << 2) + gear_ls_ptr[buf[pos]])
		if (gear_hash & config.mask_l_ls) == 0:
			return _CutResult(gear_hash, pos)
		gear_hash = gear_hash + gear_ptr[buf[pos + 1]]
		if (gear_hash & config.mask_l) == 0:
			return _CutResult(gear_hash, pos + 1)
		idx += 1

	return _CutResult(gear_hash, remaining)


cdef class _BufferChunkSpliter:
	cdef object fastcdc
	cdef const _Config * config
	cdef memoryview buf
	cdef uint64_t offset

	def __init__(self, fastcdc: FastCDC2020, buf: memoryview):
		self.fastcdc = fastcdc  # keep ref
		self.config = &fastcdc.config
		self.buf = buf
		self.offset = 0

	def __next__(self) -> Chunk:
		cdef const uint8_t[:] sub_buf = self.buf[self.offset:]
		cdef uint64_t sub_buf_len = sub_buf.shape[0]
		if sub_buf_len == 0:
			raise StopIteration()

		cdef _CutResult res = _cut_gear(self.config, &sub_buf[0], sub_buf.shape[0])
		cdef uint64_t end_pos = self.offset + res.cut_offset

		chunk = Chunk(
			hash=res.gear_hash,
			offset=self.offset,
			length=res.cut_offset,
			data=memoryview(self.buf[self.offset:end_pos]),
		)

		self.offset += res.cut_offset
		return chunk

	def __iter__(self):
		return self


cdef class _FileChunkSpliter(_BufferChunkSpliter):
	cdef object mmap_obj
	cdef object file_obj

	def __init__(self, fastcdc: FastCDC2020, file: io.BufferedReader, file_size: int):
		self.file_obj = file
		if file_size == 0:
			self.mmap_obj = None
			mv = memoryview(b'')
		else:
			self.mmap_obj = mmap.mmap(file.fileno(), file_size, access=mmap.ACCESS_READ)
			mv = memoryview(self.mmap_obj)
		_BufferChunkSpliter.__init__(self, fastcdc, mv)

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.close()

	def close(self):
		self.file_obj.close()


_ReadintoFunc = Callable[[memoryview], int]


cdef class _StreamChunkSpliter:
	cdef object fastcdc
	cdef const _Config * config
	cdef object readinto_func

	cdef uint64_t offset
	cdef uint64_t last_chunk_len
	cdef uint64_t buf_capacity
	cdef bytearray buf_obj
	cdef uint8_t[:] buf_view
	cdef uint64_t buf_len

	def __init__(self, fastcdc: FastCDC2020, readinto_func: _ReadintoFunc):
		self.fastcdc = fastcdc  # keep ref
		self.config = &fastcdc.config
		self.readinto_func = readinto_func

		self.offset = 0
		self.last_chunk_len = 0
		self.buf_capacity = fastcdc.config.max_size
		self.buf_obj = bytearray(self.buf_capacity)
		self.buf_view = self.buf_obj
		self.buf_len = 0

	def __next__(self) -> Chunk:
		cdef uint64_t remaining_buf_len
		cdef uint8_t* buf_ptr = &self.buf_view[0]

		if self.last_chunk_len > 0:
			if self.last_chunk_len > self.buf_len:
				raise AssertionError(f'last chunk length {self.last_chunk_len} is greater than buffer length {self.buf_len}')
			remaining_buf_len = self.buf_len - self.last_chunk_len
			memmove(buf_ptr, buf_ptr + self.last_chunk_len, remaining_buf_len)
			self.buf_len = remaining_buf_len
			self.offset += self.last_chunk_len
			self.last_chunk_len = 0

		if self.buf_len < self.buf_capacity:
			while self.buf_len < self.buf_capacity:
				n_read = self.readinto_func(memoryview(self.buf_obj)[self.buf_len:])
				if n_read == 0:
					break
				self.buf_len += n_read
		if self.buf_len == 0:
			raise StopIteration()

		cdef _CutResult res = _cut_gear(self.config, buf_ptr, self.buf_len)
		cdef uint64_t chunk_len = res.cut_offset
		if chunk_len == 0:  # last part of the file
			chunk_len = self.buf_len

		self.last_chunk_len = chunk_len
		return Chunk(
			hash=res.gear_hash,
			offset=self.offset,
			length=chunk_len,
			data=memoryview(self.buf_obj)[:chunk_len]
		)

	def __iter__(self):
		return self
