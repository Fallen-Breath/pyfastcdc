import os
import subprocess
import sys
import sysconfig
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from threading import Barrier

import pytest

from pyfastcdc import FastCDC


@pytest.mark.skipif(not sysconfig.get_config_var('Py_GIL_DISABLED'), reason='requires free-threaded Python')
def test_import_keeps_gil_disabled():
	# Check the first import in a fresh process, without forcing the GIL off.
	env = os.environ.copy()
	env.pop('PYTHON_GIL', None)
	result = subprocess.run(
		[sys.executable, '-c', (
			'import sys; assert not sys._is_gil_enabled(); '
			'import pyfastcdc; assert not sys._is_gil_enabled()'
		)],
		env=env, capture_output=True, text=True, timeout=30,
	)
	assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('method', ['buf', 'stream'])
def test_independent_instances_in_parallel(method, random_data_1m):
	def collect(seed, data):
		cdc = FastCDC(avg_size=4096, seed=seed)
		chunks = cdc.cut_buf(data) if method == 'buf' else cdc.cut_stream(BytesIO(data))
		# Copy each stream chunk before advancing the iterator and reusing its buffer.
		return [(chunk.offset, chunk.length, chunk.gear_hash, bytes(chunk.data)) for chunk in chunks]

	cases = [(seed, bytes([seed]) + random_data_1m) for seed in (0, 1)]
	expected = [collect(*case) for case in cases]
	start = Barrier(len(cases), timeout=30)
	free_threaded = bool(sysconfig.get_config_var('Py_GIL_DISABLED'))

	def worker(case):
		start.wait()
		if free_threaded:
			assert not sys._is_gil_enabled()
		result = collect(*case)
		if free_threaded:
			assert not sys._is_gil_enabled()
		return result

	with ThreadPoolExecutor(max_workers=len(cases)) as pool:
		futures = [pool.submit(worker, case) for case in cases]
		actual = [future.result() for future in futures]
	assert actual == expected
