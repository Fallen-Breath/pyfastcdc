__version__ = '0.0.1'

__all__ = [
	'BinaryStreamReader',
	'Chunk',
	'FastCDC',
	'NormalizedChunking',
]

from fastcdc2020.common import (
	BinaryStreamReader,
	Chunk,
	NormalizedChunking,
)

try:
	from fastcdc2020.cy import FastCDC
except ImportError:
	from fastcdc2020.py import FastCDC
	import warnings
	warnings.warn('Failed to import fastcdc2020 cython extension, fallback to pure python implementation with is a lot slower.')
