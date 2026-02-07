import dataclasses
from pathlib import Path
from typing import Optional, ClassVar, Union, Iterator, List, ContextManager

from fastcdc2020 import utils, NormalizedChunking, Chunk, BinaryStreamReader, ChunkIterator, FileHoldingChunkIterator
from fastcdc2020.py.constants import MASKS, GEAR, GEAR_LS
from fastcdc2020.utils import ReadintoFunc

_UINT64_MASK = (1 << 64) - 1


@dataclasses.dataclass(frozen=True)
class _Config:
	avg_size: int
	min_size: int
	max_size: int
	normalized_chunking: NormalizedChunking
	mask_s: int
	mask_l: int
	mask_s_ls: int
	mask_l_ls: int
	gear: List[int]
	gear_ls: List[int]


class FastCDC:
	MIN_SIZE_LOWER_BOUND: ClassVar[int] = 64
	AVG_SIZE_LOWER_BOUND: ClassVar[int] = 256
	MAX_SIZE_LOWER_BOUND: ClassVar[int] = 1024
	MIN_SIZE_UPPER_BOUND: ClassVar[int] = 1 * 1048576
	AVG_SIZE_UPPER_BOUND: ClassVar[int] = 4 * 1048576
	MAX_SIZE_UPPER_BOUND: ClassVar[int] = 16 * 1048576

	def __init__(
			self,
			avg_size: int = 16384,
			*,
			min_size: Optional[int] = None,
			max_size: Optional[int] = None,
			normalized_chunking: NormalizedChunking = 1,
			seed: int = 0,
	):
		if min_size is None:
			min_size = avg_size // 4
		if max_size is None:
			max_size = avg_size * 4
		if not (self.AVG_SIZE_LOWER_BOUND <= avg_size <= self.AVG_SIZE_UPPER_BOUND):
			raise ValueError(f'min_size {avg_size} is out of range [{self.AVG_SIZE_LOWER_BOUND}, {self.AVG_SIZE_UPPER_BOUND}]')
		if not (self.MIN_SIZE_LOWER_BOUND <= min_size <= self.MIN_SIZE_UPPER_BOUND):
			raise ValueError(f'min_size {min_size} is out of range [{self.MIN_SIZE_LOWER_BOUND}, {self.MIN_SIZE_UPPER_BOUND}]')
		if not (self.MAX_SIZE_LOWER_BOUND <= max_size <= self.MAX_SIZE_UPPER_BOUND):
			raise ValueError(f'min_size {max_size} is out of range [{self.MAX_SIZE_LOWER_BOUND}, {self.MAX_SIZE_UPPER_BOUND}]')
		if not (min_size <= avg_size <= max_size):
			raise ValueError(f'avg_size {avg_size} is out of range [{min_size}, {max_size}]')

		bits = avg_size.bit_length() - 1
		mask_s = MASKS[bits + normalized_chunking]
		mask_l = MASKS[bits - normalized_chunking]
		mask_s_ls = (mask_s << 1) & _UINT64_MASK
		mask_l_ls = (mask_l << 1) & _UINT64_MASK

		gear = GEAR
		gear_ls = GEAR_LS
		if seed > 0:
			gear = [(x ^ seed) & _UINT64_MASK for x in GEAR]
			gear_ls = [(x ^ seed) & _UINT64_MASK for x in GEAR_LS]

		self.config = _Config(
			avg_size=avg_size,
			min_size=min_size,
			max_size=max_size,
			normalized_chunking=normalized_chunking,
			mask_s=mask_s,
			mask_l=mask_l,
			mask_s_ls=mask_s_ls,
			mask_l_ls=mask_l_ls,
			gear=gear,
			gear_ls=gear_ls,
		)

	def cut_buf(self, buf: Union[bytes, bytearray, memoryview]) -> ChunkIterator:
		return BufferChunkSplitter(self.config, utils.create_memoryview_from_buffer(buf))

	def cut_file(self, file_path: Union[str, bytes, Path]) -> FileHoldingChunkIterator:
		return FileChunkSplitter(self.config, file_path)

	def cut_stream(self, stream: BinaryStreamReader) -> ChunkIterator:
		return StreamChunkSplitter(self.config, utils.create_readinto_func(stream))


@dataclasses.dataclass(frozen=True)
class _CutResult:
	gear_hash: int
	cut_offset: int


def _cut_gear(config: _Config, buf: memoryview) -> _CutResult:
	remaining = len(buf)
	if remaining <= config.min_size:
		return _CutResult(0, remaining)
	center = config.avg_size
	if remaining > config.max_size:
		remaining = config.max_size
	elif remaining < center:
		center = remaining

	idx = config.min_size // 2
	gear_hash = 0
	gear = config.gear
	gear_ls = config.gear_ls

	while idx < center // 2:
		pos = idx * 2
		gear_hash = ((gear_hash << 2) + gear_ls[buf[pos]]) & _UINT64_MASK
		if (gear_hash & config.mask_s_ls) == 0:
			return _CutResult(gear_hash, pos)
		gear_hash = (gear_hash + gear[buf[pos + 1]]) & _UINT64_MASK
		if (gear_hash & config.mask_s) == 0:
			return _CutResult(gear_hash, pos + 1)
		idx += 1

	while idx < remaining // 2:
		pos = idx * 2
		gear_hash = ((gear_hash << 2) + gear_ls[buf[pos]]) & _UINT64_MASK
		if (gear_hash & config.mask_l_ls) == 0:
			return _CutResult(gear_hash, pos)
		gear_hash = (gear_hash + gear[buf[pos + 1]]) & _UINT64_MASK
		if (gear_hash & config.mask_l) == 0:
			return _CutResult(gear_hash, pos + 1)
		idx += 1

	return _CutResult(gear_hash, remaining)


class BufferChunkSplitter(Iterator[Chunk]):
	def __init__(self, config: _Config, buf: memoryview):
		self.config = config
		self.buf = buf
		self.offset = 0

	def __next__(self) -> Chunk:
		if self.offset >= len(self.buf):
			raise StopIteration()

		res = _cut_gear(self.config, self.buf[self.offset:])
		end_pos = self.offset + res.cut_offset

		chunk = Chunk(
			hash=res.gear_hash,
			offset=self.offset,
			length=res.cut_offset,
			data=self.buf[self.offset:end_pos],
		)
		self.offset += res.cut_offset
		return chunk


class FileChunkSplitter(BufferChunkSplitter, ContextManager['FileChunkSplitter']):
	def __init__(self, config: _Config, file_path: Union[str, bytes, Path]):
		self.mmap_file = utils.create_mmap_from_file(file_path)
		super().__init__(config, self.mmap_file.data)

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.close()

	def close(self):
		self.mmap_file.close()


class StreamChunkSplitter(Iterator[Chunk]):
	def __init__(self, config: _Config, readinto_func: ReadintoFunc):
		self.config = config
		self.readinto_func = readinto_func

		self.offset = 0
		self.last_chunk_len = 0
		self.buf = bytearray(config.max_size)
		self.buf_len = 0

	def __next__(self) -> Chunk:
		if self.last_chunk_len > 0:
			if self.last_chunk_len > self.buf_len:
				raise AssertionError(f'last chunk length {self.last_chunk_len} is greater than buffer length {self.buf_len}')
			remaining_buf_len = self.buf_len - self.last_chunk_len
			self.buf[:remaining_buf_len] = self.buf[self.last_chunk_len:self.buf_len]
			self.buf_len = remaining_buf_len
			self.offset += self.last_chunk_len
			self.last_chunk_len = 0

		if self.buf_len < len(self.buf):
			while self.buf_len < len(self.buf):
				n_read = self.readinto_func(memoryview(self.buf)[self.buf_len:])
				if n_read == 0:
					break
				self.buf_len += n_read
		if self.buf_len == 0:
			raise StopIteration()

		res = _cut_gear(self.config, memoryview(self.buf)[:self.buf_len])
		chunk_len = res.cut_offset
		if res.cut_offset <= 0:  # last part of the file
			chunk_len = self.buf_len

		self.last_chunk_len = chunk_len
		return Chunk(
			hash=res.gear_hash,
			offset=self.offset,
			length=chunk_len,
			data=memoryview(self.buf)[:chunk_len]
		)
