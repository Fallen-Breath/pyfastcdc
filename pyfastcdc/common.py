from typing import Union

from typing_extensions import Literal, Protocol


class _BinaryStreamReaderWithRead(Protocol):
	def read(self, n: int) -> bytes: ...


class _BinaryStreamReaderWithReadinto(Protocol):
	def readinto(self, b: memoryview) -> int: ...


BinaryStreamReader = Union[_BinaryStreamReaderWithRead, _BinaryStreamReaderWithReadinto]
NormalizedChunking = Literal[0, 1, 2, 3]
