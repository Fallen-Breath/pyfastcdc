from pathlib import Path
from typing import List

from Cython.Build import cythonize
from setuptools import setup, find_packages

HERE = Path(__file__).absolute().parent


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


def get_version() -> str:
	from fastcdc2020 import __version__
	return __version__


setup(
	name='fastcdc2020',
	version=get_version(),
	description='FastCDC 2020 implementation in Python',
	long_description=read_file('README.md'),
	long_description_content_type='text/markdown',
	author='Fallen_Breath',
	python_requires='>=3.6',
	project_urls={
		'Homepage': 'https://github.com/Fallen-Breath/fastcdc2020-py',
	},

	packages=find_packages(exclude=['tests', '*.tests', '*.tests.*', 'tests.*']),
	include_package_data=True,

	install_requires=read_requirements('requirements.txt'),
	extras_require={
		'dev': read_requirements('requirements.dev.txt'),
	},
	classifiers=[
		'License :: OSI Approved :: MIT License',
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

	ext_modules=cythonize(
		'fastcdc2020/cy/*.pyx',
		compiler_directives={'language_level': '3'}
	),
)
