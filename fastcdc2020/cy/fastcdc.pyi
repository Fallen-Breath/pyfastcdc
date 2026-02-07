from pathlib import Path
from typing import Optional, Union, Iterator

from fastcdc2020.common import NormalizedChunking, BinaryStreamReader, Chunk


class FastCDC:
	def __init__(
			self,
			avg_size: int = 16384,
			*,
			min_size: Optional[int] = None,
			max_size: Optional[int] = None,
			normalized_chunking: NormalizedChunking = 1,
			seed: int = 0,
	):
		...

	def cut_buf(self, buf: Union[bytes, bytearray, memoryview]) -> Iterator[Chunk]:
		...

	def cut_file(self, file_path: Union[str, bytes, Path]) -> Iterator[Chunk]:
		...

	def cut_stream(self, stream: BinaryStreamReader) -> Iterator[Chunk]:
		...
