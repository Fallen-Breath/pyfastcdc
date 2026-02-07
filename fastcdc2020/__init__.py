__version__ = '0.0.1'

from fastcdc2020.common import BinaryStreamReader
from fastcdc2020.common import Chunk
from fastcdc2020.common import ChunkIterator
from fastcdc2020.common import FileHoldingChunkIterator
from fastcdc2020.common import NormalizedChunking

try:
	from fastcdc2020.cy.fastcdc import FastCDC2020
except ImportError:
	from fastcdc2020.py.fastcdc import FastCDC2020
