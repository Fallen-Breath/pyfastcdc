import argparse
import csv
import functools
import time
from pathlib import Path
from typing import Union, Callable, Dict, Type, List

import numpy as np

from pyfastcdc.cy import FastCDC as FastCDC_cy
from pyfastcdc.py import FastCDC as FastCDC_py

FastCDC = Union[FastCDC_cy, FastCDC_py]
HERE = Path(__file__).absolute().parent
DEFAULT_BENCHMARK_DIR = HERE / 'benchmark'


def create_random_file(filename: Path, size: int, seed: int):
	rng = np.random.default_rng(seed)
	chunk_size = 1024 * 1024

	with open(filename, 'wb') as f:
		remaining = size
		while remaining > 0:
			current = min(chunk_size, remaining)
			chunk = rng.integers(0, 256, size=current, dtype=np.uint8).tobytes()
			f.write(chunk)
			remaining -= current


def init_benchmark_dir(benchmark_dir: Path):
	benchmark_dir.mkdir(parents=True, exist_ok=True)
	gitignore_file = benchmark_dir / '.gitignore'
	if not gitignore_file.exists():
		gitignore_file.write_text('**\n')


@functools.lru_cache(maxsize=1)
def read_file_cached(file_name: Path) -> bytes:
	return file_name.read_bytes()


class TestChunkerFunction:
	def __init__(self, cdc: FastCDC, file_name: Path):
		self.cdc = cdc
		self.file_name = file_name

	def init(self): pass
	def run(self): pass


class TestCutBuf(TestChunkerFunction):
	buf = b''

	def init(self):
		self.buf = read_file_cached(self.file_name)

	def run(self):
		for _ in self.cdc.cut_buf(self.buf):
			pass


class TestCutFile(TestChunkerFunction):
	def run(self):
		for _ in self.cdc.cut_file(self.file_name):
			pass


class TestCutStream(TestChunkerFunction):
	def run(self):
		with open(self.file_name, 'rb') as f:
			for _ in self.cdc.cut_stream(f):
				pass


def measure_time_cost(func: Callable[[], None], round_cnt: int) -> float:
	start_time = time.time()
	for _ in range(round_cnt):
		func()
	return (time.time() - start_time) / round_cnt


def ensure_random_file(file_path: Path, size: int, seed: int = 0):
	if file_path.exists() and file_path.stat().st_size == size:
		return file_path
	print(f'Creating size={size} random file...')
	create_random_file(file_path, size, seed)
	return file_path


def benchmark(benchmark_dir: Path, output_csv_path: Path, test_files: List[str]):
	if 'rand_100M.bin' in test_files:
		ensure_random_file(benchmark_dir / 'rand_100M.bin', 100 * 1024 * 1024, 0)
	if 'rand_1G.bin' in test_files:
		ensure_random_file(benchmark_dir / 'rand_1G.bin', 1024 * 1024 * 1024, 0)
	if 'rand_10G.bin' in test_files:
		ensure_random_file(benchmark_dir / 'rand_10G.bin', 10 * 1024 * 1024 * 1024, 0)

	test_files = [benchmark_dir / name for name in test_files]
	avg_sizes = [
		4 * 1024,
		8 * 1024,
		16 * 1024,
		32 * 1024,
		64 * 1024,
		128 * 1024,
		256 * 1024,
		512 * 1024,
		1024 * 1024,
		2 * 1024 * 1024,
		4 * 1024 * 1024,
	]
	impl_classes: Dict[str, Type[FastCDC]] = {
		'cy': FastCDC_cy,
		# 'py': FastCDC_py,
	}
	chunker_funcs: Dict[str, Type[TestChunkerFunction]] = {
		'cut_buf': TestCutBuf,
		'cut_file': TestCutFile,
		'cut_stream': TestCutStream,
	}

	with open(output_csv_path, 'w', encoding='utf8', newline='') as f:
		writer = csv.DictWriter(f, fieldnames=['file_name', 'file_size', 'avg_size', 'impl', 'func', 'cost_ms', 'mib_per_sec'])
		writer.writeheader()

		for test_file_path in test_files:
			for avg_size in avg_sizes:
				for impl_name, impl_class in impl_classes.items():
					cdc = impl_class(avg_size)
					for chunker_name, chunker_func_type in chunker_funcs.items():
						chunker_func = chunker_func_type(cdc, test_file_path)
						chunker_func.init()
						cost_sec = measure_time_cost(chunker_func.run, 5)
						file_size = test_file_path.stat().st_size
						mib_per_sec = file_size / cost_sec / 1024 / 1024
						row = {
							'file_name': test_file_path.name,
							'file_size': file_size,
							'avg_size': avg_size,
							'impl': impl_name,
							'func': chunker_name,
							'cost_ms': round(cost_sec * 1000, 6),
							'mib_per_sec': round(mib_per_sec, 6),
						}
						print(row)
						writer.writerow(row)
			read_file_cached.cache_clear()


def main():
	parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
	parser.add_argument('--test-files', nargs='+', default=['rand_100M.bin', 'rand_1G.bin', 'rand_10G.bin'])
	parser.add_argument('--benchmark-dir', type=Path, default=DEFAULT_BENCHMARK_DIR)
	parser.add_argument('--output-csv', type=Path, default=DEFAULT_BENCHMARK_DIR / 'result.csv')
	args = parser.parse_args()

	init_benchmark_dir(args.benchmark_dir)
	benchmark(args.benchmark_dir, args.output_csv, args.test_files)


if __name__ == '__main__':
	main()
