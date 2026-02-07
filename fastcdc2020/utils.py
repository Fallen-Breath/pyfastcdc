import io
import mmap
import os
from pathlib import Path
from typing import Callable, Union, Optional

from fastcdc2020.common import BinaryStreamReader

ReadintoFunc = Callable[[memoryview], int]


def create_memoryview_from_buffer(buf: Union[bytes, bytearray, memoryview]) -> memoryview:
	if isinstance(buf, (bytes, bytearray, memoryview)):
		return memoryview(buf)
	raise TypeError('buf must be bytes or bytearray')


def create_readinto_func(stream: BinaryStreamReader) -> ReadintoFunc:
	readinto_func: ReadintoFunc = getattr(stream, 'readinto', None)
	if readinto_func is not None and callable(readinto_func):
		return readinto_func

	read_func = getattr(stream, 'read', None)
	if read_func is not None and callable(read_func):
		def readinto_using_read(dest_buf: memoryview) -> int:
			read_buf = read_func(len(dest_buf))
			dest_buf[:len(read_buf)] = read_buf
			return len(read_buf)

		return readinto_using_read

	raise TypeError('stream must be readable')


class MmapFile:
	def __init__(self, file_path: Union[str, bytes, Path]):
		self.file_obj: Optional[io.BufferedReader] = None
		self.mmap_obj: Optional[mmap.mmap] = None
		self.data = memoryview(b'')
		self.__open(file_path)

	def __open(self, file_path: Union[str, bytes, Path]):
		file_size = os.path.getsize(file_path)
		self.file_obj = open(file_path, 'rb')

		if file_size == 0:
			self.close()
			return

		try:
			self.mmap_obj = mmap.mmap(self.file_obj.fileno(), length=file_size, access=mmap.ACCESS_READ)
			self.data = memoryview(self.mmap_obj)
		except:
			self.close()
			raise

	def close(self):
		# If we close mmap_obj here, "BufferError: cannot close exported pointers exist" will possibly be thrown,
		# since the caller might still have reference of the output Chunk object in local variables,
		# which contains memoryview reference to this mmap_obj.
		# So just don't close the mmap_obj here, let GC handle it
		if self.file_obj and not self.file_obj.closed:
			self.file_obj.close()
		self.file_obj = None

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.close()


def create_mmap_from_file(file_path: Union[str, bytes, Path]) -> MmapFile:
	return MmapFile(file_path)
