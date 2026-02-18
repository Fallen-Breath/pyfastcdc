__version__ = '0.1.1b2'

__all__ = [
	'BinaryStreamReader',
	'Chunk',
	'FastCDC',
	'NormalizedChunking',
]

from pyfastcdc.common import (
	BinaryStreamReader,
	Chunk,
	NormalizedChunking,
)

try:
	from pyfastcdc.cy import FastCDC
except ImportError:
	from pyfastcdc.py import FastCDC
	import warnings
	warnings.warn('Failed to import pyfastcdc cython extension, fallback to pure python implementation with is a lot slower.')
