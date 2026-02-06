import dataclasses
import mmap
from io import BufferedReader
from pathlib import Path
from typing import Optional, ClassVar, Union, Iterator, List, ContextManager, Tuple
from typing_extensions import Literal, Protocol

from fastcdc2020.py.constants import MASKS, GEAR, GEAR_LS

NormalizedChunking = Literal[0, 1, 2, 3]
_UINT64_MASK = (1 << 64) - 1


class _ReadableBinaryStream(Protocol):
	# def read(self, n: int, /) -> bytes: ...
	def readinto(self, b: Union[bytearray, memoryview]) -> int: ...


@dataclasses.dataclass(frozen=True)
class Chunk:
	hash: int  # FastCDC's gear hash
	offset: int
	length: int
	data: memoryview


@dataclasses.dataclass(frozen=True)
class _FastCDC2020Config:
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


def _cut_gear(config: _FastCDC2020Config, buf: memoryview) -> Tuple[int, int]:  # (gear_hash, offset)
	remaining = len(buf)
	if remaining <= config.min_size:
		return 0, remaining
	center = config.avg_size
	if remaining > config.max_size:
		remaining = config.max_size
	elif remaining < center:
		center = remaining

	idx = config.min_size // 2
	gear_hash = 0

	while idx < center // 2:
		a = idx * 2
		gear_hash = ((gear_hash << 2) + config.gear_ls[buf[a]]) & _UINT64_MASK
		if (gear_hash & config.mask_s_ls) == 0:
			return gear_hash, a
		gear_hash = (gear_hash + config.gear[buf[a + 1]]) & _UINT64_MASK
		if (gear_hash & config.mask_s) == 0:
			return gear_hash, a + 1
		idx += 1

	while idx < remaining // 2:
		a = idx * 2
		gear_hash = ((gear_hash << 2) + config.gear_ls[buf[a]]) & _UINT64_MASK
		if (gear_hash & config.mask_l_ls) == 0:
			return gear_hash, a
		gear_hash = (gear_hash + config.gear[buf[a + 1]]) & _UINT64_MASK
		if (gear_hash & config.mask_l) == 0:
			return gear_hash, a + 1
		idx += 1

	return gear_hash, remaining


class _BufferChunkSpliter(Iterator[Chunk]):
	def __init__(self, config: _FastCDC2020Config, buf: memoryview):
		self.config = config
		self.buf = buf
		self.offset = 0
		self.reached_end = False

	def __next__(self) -> Chunk:
		if self.reached_end:
			raise StopIteration()
		gear_hash, chunk_len = _cut_gear(self.config, self.buf[self.offset:])
		data = self.buf[self.offset:self.offset + chunk_len]
		chunk = Chunk(hash=gear_hash, offset=self.offset, length=chunk_len, data=data)
		self.offset += chunk_len
		self.reached_end = self.offset >= len(self.buf)
		return chunk


class _StreamChunkSpliter(Iterator[Chunk]):
	def __init__(self, config: _FastCDC2020Config, stream: _ReadableBinaryStream):
		self.config = config
		self.stream = stream
		self.offset = 0
		self.last_chunk_len = 0
		self.buf = bytearray(config.max_size)
		self.buf_size = 0

	def __next__(self) -> Chunk:
		if self.last_chunk_len > 0:
			assert self.last_chunk_len <= self.buf_size
			remaining_buf_len = self.buf_size - self.last_chunk_len
			self.buf[:remaining_buf_len] = self.buf[self.last_chunk_len:self.buf_size]
			self.buf_size -= self.last_chunk_len
			self.offset += self.last_chunk_len
			self.last_chunk_len = 0

		if self.buf_size < len(self.buf):
			while self.buf_size < len(self.buf):
				n_read = self.stream.readinto(memoryview(self.buf)[self.buf_size:])
				if n_read == 0:
					break
				self.buf_size += n_read
			self.eof = self.buf_size < len(self.buf)
		if self.buf_size == 0:
			raise StopIteration()

		gear_hash, chunk_len = _cut_gear(self.config, memoryview(self.buf)[:self.buf_size])
		if chunk_len <= 0:  # last part of the file
			chunk_len = self.buf_size
		self.last_chunk_len = chunk_len
		return Chunk(hash=gear_hash, offset=self.offset, length=chunk_len, data=memoryview(self.buf)[:chunk_len])


class _FileStreamChunkSpliter(_BufferChunkSpliter, ContextManager['_FileStreamChunkSpliter']):
	def __init__(self, config: _FastCDC2020Config, file: BufferedReader):
		try:
			m = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
		except ValueError as e:
			if type(e) is ValueError and str(e) == 'cannot mmap an empty file':
				mv = memoryview(b'')
			else:
				raise
		else:
			mv = memoryview(m)
		super().__init__(config, mv)
		self.file = file

	def __enter__(self) -> '_FileStreamChunkSpliter':
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.close()

	def close(self):
		self.file.close()


class FastCDC2020:
	MIN_SIZE_LOWER_BOUND: ClassVar[int] = 64
	AVG_SIZE_LOWER_BOUND: ClassVar[int] = 256
	MAX_SIZE_LOWER_BOUND: ClassVar[int] = 1024
	MIN_SIZE_UPPER_BOUND: ClassVar[int] = 1 * 1048576
	AVG_SIZE_UPPER_BOUND: ClassVar[int] = 4 * 1048576
	MAX_SIZE_UPPER_BOUND: ClassVar[int] = 16 * 1048576

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
			min_size = avg_size // 2
		if max_size is None:
			max_size = avg_size * 2
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

		self.config = _FastCDC2020Config(
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

	def cut_buf(self, buf: Union[bytes, bytearray, memoryview]) -> _BufferChunkSpliter:
		if isinstance(buf, (bytes, bytearray)):
			buf = memoryview(buf)
		return _BufferChunkSpliter(self.config, buf)

	def cut_stream(self, stream: _ReadableBinaryStream) -> _StreamChunkSpliter:
		return _StreamChunkSpliter(self.config, stream)

	def cut_file(self, path: Union[str, bytes, Path]) -> _FileStreamChunkSpliter:
		return _FileStreamChunkSpliter(self.config, open(path, 'rb'))
