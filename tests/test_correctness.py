import hashlib
import io
import os
import random
import tempfile
from pathlib import Path
from typing import Dict, Tuple

import pytest

from pyfastcdc import FastCDC as FastCDC_cy
from pyfastcdc.common import NormalizedChunking
from pyfastcdc.py import FastCDC as FastCDC_py
from tests.utils import FastCDCType


class TestSekienAkashitaImage:
	# (avg_size, seed) -> [(gear_hash, length), ...]
	EXPECTED_RESULT = {
		# (16384, 0): [
        #     (17968276318003433923, 21325),
        #     (8197189939299398838, 17140),
        #     (13019990849178155730, 28084),
        #     (4509236223063678303, 18217),
        #     (2504464741100432583, 24700),
        # ],
		(16384, 666): [
            (9312357714466240148, 10605),
            (226910853333574584, 55745),
            (12271755243986371352, 11346),
            (14153975939352546047, 5883),
            (5890158701071314778, 11586),
            (8981594897574481255, 14301),
        ],
		# (32768, 0): [
		# 	(15733367461443853673, 66549),
		# 	(6321136627705800457, 42917),
		# ],
		# (65536, 0): [
		# 	(2504464741100432583, 109466),
		# ],
	}

	@pytest.mark.parametrize('case_param', EXPECTED_RESULT.keys())
	def test_sekien_akashita(self, fastcdc_impl: FastCDCType, sekien_akashita_bytes: bytes, case_param: Tuple[int, int]):
		avg_size, seed = case_param
		expected = self.EXPECTED_RESULT[case_param]

		h = hashlib.sha256()
		chunks = list(fastcdc_impl(avg_size=avg_size, seed=seed).cut_buf(sekien_akashita_bytes))

		assert len(chunks) == len(expected)
		for chunk, (gear_hash, length) in zip(chunks, expected):
			assert chunk.gear_hash == gear_hash
			assert chunk.length == length
			h.update(chunk.data)
		assert h.hexdigest() == hashlib.sha256(sekien_akashita_bytes).hexdigest()


class TestPyCyConsistency:
	DATA_SIZES = [
		*range(100),
		*[i * 100 for i in range(1, 10)],
		*[i * 1024 for i in range(1, 10)],
		*[i * 10 * 1024 for i in range(1, 5)],
		*[i * 100 * 1024 for i in range(1, 4)],
		*[i * 1024 * 1024 for i in range(1, 3)],
	]
	AVG_SIZES = [1024, 4096, 8192, 12345, 16384, 65536]

	@pytest.fixture(scope="class")
	def random_data_by_size(self) -> Dict[int, bytes]:
		data_cache = {}
		for size in self.DATA_SIZES:
			rnd = random.Random(size)
			data_cache[size] = bytes(rnd.getrandbits(8) for _ in range(size))
		return data_cache

	@pytest.mark.parametrize('avg_size', AVG_SIZES)
	@pytest.mark.parametrize('data_size', DATA_SIZES)
	@pytest.mark.parametrize('normalized_chunking', [0, 1, 2, 3])
	@pytest.mark.parametrize('seed', [0, 1])
	def test_py_cy_consistency(self, avg_size: int, data_size: int, normalized_chunking: NormalizedChunking, seed: int, random_data_by_size: Dict[int, bytes]):
		data = random_data_by_size[data_size]

		cdc_py = FastCDC_py(avg_size=avg_size, normalized_chunking=normalized_chunking, seed=seed)
		cdc_cy = FastCDC_cy(avg_size=avg_size, normalized_chunking=normalized_chunking, seed=seed)
		assert cdc_py.avg_size == cdc_cy.avg_size
		assert cdc_py.min_size == cdc_cy.min_size
		assert cdc_py.max_size == cdc_cy.max_size

		chunks_py = list(cdc_py.cut_buf(data))
		chunks_cy = list(cdc_cy.cut_buf(data))

		assert len(chunks_py) == len(chunks_cy)

		for py_chunk, cy_chunk in zip(chunks_py, chunks_cy):
			assert py_chunk.offset == cy_chunk.offset
			assert py_chunk.length == cy_chunk.length
			assert py_chunk.gear_hash == cy_chunk.gear_hash
			assert bytes(py_chunk.data) == bytes(cy_chunk.data)

		reconstructed_py = b''.join(bytes(chunk.data) for chunk in chunks_py)
		reconstructed_cy = b''.join(bytes(chunk.data) for chunk in chunks_cy)
		assert reconstructed_py == data
		assert reconstructed_cy == data


class TestCutMethods:
	def test_chunk_properties(self, fastcdc_instance, random_data_1m: bytes):
		prev_offset = None
		prev_size = None
		for chunk in fastcdc_instance.cut_buf(random_data_1m):
			assert isinstance(chunk.gear_hash, int)
			assert isinstance(chunk.offset, int)
			assert isinstance(chunk.length, int)
			assert isinstance(chunk.data, memoryview)

			assert len(chunk.data) == chunk.length

			if prev_offset is not None:
				assert chunk.offset == prev_offset + prev_size
			prev_offset = chunk.offset
			prev_size = chunk.length

	def test_cut_buf_vs_cut_file_consistency(self, fastcdc_impl: FastCDCType, random_data_1m: bytes, tmp_path: Path):
		cdc = fastcdc_impl(avg_size=8192)
		chunks_memory = list(cdc.cut_buf(random_data_1m))

		temp_file = tmp_path / 'test.bin'
		temp_file.write_bytes(random_data_1m)

		chunk_cnt = 0
		for i, chunk in enumerate(cdc.cut_file(temp_file)):
			chunk_cnt += 1
			assert chunk_cnt <= len(chunks_memory)
			assert chunk.offset == chunks_memory[i].offset
			assert chunk.length == chunks_memory[i].length
			assert chunk.gear_hash == chunks_memory[i].gear_hash
			assert bytes(chunk.data) == bytes(chunks_memory[i].data)
		assert len(chunks_memory) == chunk_cnt

	def test_cut_buf_vs_cut_stream_consistency_read(self, fastcdc_impl: FastCDCType, random_data_1m: bytes):
		cdc = fastcdc_impl(avg_size=8192)
		chunks_memory = list(cdc.cut_buf(random_data_1m))
		bytes_io = io.BytesIO(random_data_1m)

		class MyStream:
			def read(self, n: int) -> bytes:
				return bytes_io.read(n)

		chunk_cnt = 0
		for i, chunk in enumerate(cdc.cut_stream(MyStream())):
			chunk_cnt += 1
			assert chunk_cnt <= len(chunks_memory)
			assert chunk.offset == chunks_memory[i].offset
			assert chunk.length == chunks_memory[i].length
			assert chunk.gear_hash == chunks_memory[i].gear_hash
			assert bytes(chunk.data) == bytes(chunks_memory[i].data)
		assert len(chunks_memory) == chunk_cnt

	def test_cut_buf_vs_cut_stream_consistency_readinto(self, fastcdc_impl: FastCDCType, random_data_1m: bytes):
		cdc = fastcdc_impl(avg_size=8192)
		chunks_memory = list(cdc.cut_buf(random_data_1m))
		bytes_io = io.BytesIO(random_data_1m)

		class MyStream:
			def readinto(self, buf) -> int:
				return bytes_io.readinto(buf)

		chunk_cnt = 0
		for i, chunk in enumerate(cdc.cut_stream(MyStream())):
			chunk_cnt += 1
			assert chunk_cnt <= len(chunks_memory)
			assert chunk.offset == chunks_memory[i].offset
			assert chunk.length == chunks_memory[i].length
			assert chunk.gear_hash == chunks_memory[i].gear_hash
			assert bytes(chunk.data) == bytes(chunks_memory[i].data)
		assert len(chunks_memory) == chunk_cnt


class TestSeed:
	def test_different_seeds_produce_different_chunks(self, random_data_1m: bytes):
		cdc1 = FastCDC_cy(avg_size=8192, seed=1)
		cdc2 = FastCDC_cy(avg_size=8192, seed=2)

		chunks1 = list(cdc1.cut_buf(random_data_1m))
		chunks2 = list(cdc2.cut_buf(random_data_1m))

		hashes1 = [chunk.gear_hash for chunk in chunks1]
		hashes2 = [chunk.gear_hash for chunk in chunks2]

		assert hashes1 != hashes2 or len(chunks1) == 0


class TestChunkProperties:
	def test_chunk_data_integrity(self, fastcdc_instance, random_data_1m: bytes):
		data_list = []
		for chunk in fastcdc_instance.cut_buf(random_data_1m):
			data_list.append(bytes(chunk.data))
		assert b''.join(data_list) == random_data_1m

	def test_chunk_size_constraints(self, fastcdc_impl: FastCDCType, random_data_1m: bytes):
		avg_size = 16384
		min_size = avg_size // 4  # 4096
		max_size = avg_size * 4  # 65536

		cdc = fastcdc_impl(avg_size=avg_size)
		chunks = list(cdc.cut_buf(random_data_1m))

		for chunk in chunks:
			if chunk != chunks[-1]:
				assert min_size <= chunk.length <= max_size
