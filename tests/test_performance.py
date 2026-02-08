import concurrent.futures
import multiprocessing
import time

import pytest

from pyfastcdc.cy import FastCDC


class TestGILBehavior:
	@pytest.mark.slow
	def test_cython_gil_release(self):
		cpu_cnt = multiprocessing.cpu_count()
		if cpu_cnt < 2:
			pytest.skip("Not enough CPUs to run this test")

		test_data = b'x' * (100 * 1024 * 1024)  # 100MB

		def chunk_data(data: bytes, avg_size: int) -> int:
			cdc = FastCDC(avg_size=avg_size)
			chunks = list(cdc.cut_buf(data))
			return len(chunks)

		start_time = time.time()
		results_single = []
		for i in range(4):
			results_single.append(chunk_data(test_data, 8192))
		single_thread_time = time.time() - start_time

		concurrency_count = min(cpu_cnt, 4)
		start_time = time.time()
		results_multi = []
		with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency_count) as executor:
			futures = [executor.submit(chunk_data, test_data, 8192) for _ in range(4)]
			for future in concurrent.futures.as_completed(futures):
				results_multi.append(future.result())
		multi_thread_time = time.time() - start_time

		assert sorted(results_single) == sorted(results_multi)

		# multi thread cost should be similar to single thread cost since GIL is correctly released
		assert multi_thread_time <= single_thread_time * 1.2
