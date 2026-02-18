import ast
import contextlib
import functools
import sys
from pathlib import Path
from typing import List

from setuptools import __version__ as setuptools_version
from setuptools import setup, find_packages
from setuptools.command.build_ext import build_ext

HERE = Path(__file__).absolute().parent


def parse_major(s: str) -> int:
	try:
		return int(s.split('.', 1)[0])
	except (IndexError, ValueError):
		print('Failed to parse version:', s, file=sys.stderr)
		return 0


def read_file(file_name: str) -> str:
	with open(HERE / file_name, 'r', encoding='utf8') as f:
		return f.read()


def read_requirements(file_name: str) -> List[str]:
	content = read_file(file_name)
	requirements: List[str] = []
	for line in content.splitlines():
		line = line.strip()
		if line and not line.startswith('#'):
			requirements.append(line)
	return requirements


@functools.lru_cache(None)
def get_version() -> str:
	tree = ast.parse(read_file('pyfastcdc/__init__.py'))
	for stmt in tree.body:
		if isinstance(stmt, ast.Assign) and stmt.targets[0].id == '__version__':
			if isinstance(stmt.value, ast.Constant):
				version_str = stmt.value.value
			elif hasattr(ast, 'Str') and isinstance(stmt.value, ast.Str):
				version_str = stmt.value.s
			else:
				raise TypeError(f'Unexpected type of __version__: {type(stmt.value)}')
			assert isinstance(version_str, str)
			print(f'pyfastcdc.__version__ = {version_str}')
			return version_str
	raise RuntimeError('Cannot find __version__')


class BuildExt(build_ext):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)

	@classmethod
	@contextlib.contextmanager
	def __wrap_ext_err(cls):
		try:
			yield
		except Exception as e:
			print("###########################################################################################################", file=sys.stderr)
			print("Failed to compile pyfastcdc cython extension, fallback to pure python implementation with is a lot slower", file=sys.stderr)
			print(e)
			print("###########################################################################################################", file=sys.stderr)

	def run(self):
		with self.__wrap_ext_err():
			super().run()

	def build_extensions(self):
		with self.__wrap_ext_err():
			super().build_extensions()
			

if "clean" in sys.argv or "sdist" in sys.argv:  # no cython stuffs in sdist
	# btw pandas uses this `sys.argv` hack too: https://github.com/pandas-dev/pandas/blob/78242b3f6a256e3638a655f54bb37464f13db585/setup.py#L401
	ext_modules = []
else:
	from Cython.Build import cythonize
	ext_modules = cythonize(
		'pyfastcdc/cy/*.pyx',
		compiler_directives={'language_level': '3'},
	)

print(f'setuptools_version: {setuptools_version}')
use_license_expression = parse_major(setuptools_version) >= 77
try:
	from Cython import __version__ as cython_version
except ImportError:
	cython_version = 'N/A'
print(f'Cython version: {cython_version}')

setup(
	name='pyfastcdc',
	version=get_version(),
	description='FastCDC 2020 implementation in Python, with Cython acceleration',
	long_description=read_file('README.md'),
	long_description_content_type='text/markdown',
	author='Fallen_Breath',
	python_requires='>=3.6',
	project_urls={
		'Homepage': 'https://github.com/Fallen-Breath/pyfastcdc',
	},

	packages=find_packages(exclude=['tests', '*.tests', '*.tests.*', 'tests.*']),
	include_package_data=True,

	install_requires=read_requirements('requirements.txt'),
	extras_require={
		'dev': read_requirements('requirements.dev.txt'),
	},
	**({'license': 'MIT'} if use_license_expression else {}),
	classifiers=[
		*(['License :: OSI Approved :: MIT License'] if not use_license_expression else []),
		'Programming Language :: Python',
		'Programming Language :: Python :: 3.6',
		'Programming Language :: Python :: 3.7',
		'Programming Language :: Python :: 3.8',
		'Programming Language :: Python :: 3.9',
		'Programming Language :: Python :: 3.10',
		'Programming Language :: Python :: 3.11',
		'Programming Language :: Python :: 3.12',
		'Programming Language :: Python :: 3.13',
		'Programming Language :: Python :: 3.14',
	],
	ext_modules=ext_modules,
	cmdclass={'build_ext': BuildExt},
)
