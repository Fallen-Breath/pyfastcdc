__version__ = '0.0.1'

__all__ = [
	'BinaryStreamReader',
	'Chunk',
	'ChunkIterator',
	'FileHoldingChunkIterator',
	'NormalizedChunking',
	'FastCDC',
]

from fastcdc2020.common import (
	BinaryStreamReader,
	Chunk,
	ChunkIterator,
	FileHoldingChunkIterator,
	NormalizedChunking,
)

try:
	from fastcdc2020.cy import FastCDC
except ImportError:
	from fastcdc2020.py import FastCDC
