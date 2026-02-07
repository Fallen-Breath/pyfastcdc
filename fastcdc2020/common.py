from typing import Union, Iterator

import dataclasses
from typing_extensions import Literal, Protocol


@dataclasses.dataclass(frozen=True)
class Chunk:
	hash: int  # FastCDC's gear hash
	offset: int
	length: int
	data: memoryview


class ChunkIterator(Protocol):
	def __next__(self) -> Chunk: ...
	def __iter__(self) -> Iterator[Chunk]: ...


class FileHoldingChunkIterator(ChunkIterator, Protocol):
	def __enter__(self) -> 'FileHoldingChunkIterator': ...
	def __exit__(self, exc_type, exc_val, exc_tb): ...
	def close(self): ...


class _BinaryStreamReaderWithRead(Protocol):
	def read(self, n: int) -> bytes: ...


class _BinaryStreamReaderWithReadinto(Protocol):
	def readinto(self, b: memoryview) -> int: ...


BinaryStreamReader = Union[_BinaryStreamReaderWithRead, _BinaryStreamReaderWithReadinto]
NormalizedChunking = Literal[0, 1, 2, 3]
