import dataclasses
from pathlib import Path
from typing import Optional, Literal, Union, Iterator

from typing_extensions import Protocol


@dataclasses.dataclass(frozen=True)
class Chunk:
	hash: int
	offset: int
	length: int
	data: memoryview


class _ChunkIterator(Protocol):
	def __next__(self) -> Chunk: ...
	def __iter__(self) -> Iterator[Chunk]: ...


class _FileHoldingChunkIterator(_ChunkIterator, Protocol):
	def __enter__(self) -> '_FileHoldingChunkIterator': ...
	def __exit__(self, exc_type, exc_val, exc_tb): ...
	def close(self): ...


class _ReadableBinaryStreamWithRead(Protocol):
	def read(self, n: int, /) -> bytes: ...


class _ReadableBinaryStreamWithReadinto(Protocol):
	def readinto(self, b: memoryview) -> int: ...


_ReadableBinaryStream = Union[_ReadableBinaryStreamWithRead, _ReadableBinaryStreamWithReadinto]
NormalizedChunking = Literal[0, 1, 2, 3]


class FastCDC2020:
	def __init__(
			self,
			avg_size: int = 16384,
			min_size: Optional[int] = None,
			max_size: Optional[int] = None,
			*,
			normalized_chunking: NormalizedChunking = 1,
			seed: int = 0,
	):
		...

	def cut_buf(self, buf: Union[bytes, bytearray, memoryview]) -> _ChunkIterator:
		...

	def cut_file(self, file_path: Union[str, bytes, Path]) -> _FileHoldingChunkIterator:
		...

	def cut_stream(self, stream: _ReadableBinaryStream) -> _ChunkIterator:
		...
