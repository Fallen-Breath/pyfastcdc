# PyFastCDC

[![License](https://img.shields.io/github/license/Fallen-Breath/pyfastcdc.svg)](http://www.gnu.org/licenses/gpl-3.0.html)
[![Issues](https://img.shields.io/github/issues/Fallen-Breath/pyfastcdc.svg)](https://github.com/Fallen-Breath/pyfastcdc/issues)
[![PyPI Version](https://img.shields.io/pypi/v/pyfastcdc.svg?label=PyPI)](https://pypi.org/project/pyfastcdc)

A FastCDC 2020 implementation written in Python, with [Cython](https://github.com/cython/cython) acceleration

Supports Python 3.6+. Provides prebuilt wheels for Python 3.8+

Its core algorithm implementation is a direct port of the v2020 module from [nlfiedler/fastcdc-rs](https://github.com/nlfiedler/fastcdc-rs),
which means that the output of PyFastCDC completely matches the output of nlfiedler/fastcdc-rs

## Installation

PyFastCDC is available on [PyPI](https://pypi.org/project/pyfastcdc/), with prebuilt wheels for many common platforms thanks to [cibuildwheel](https://cibuildwheel.pypa.io/)

To install, you can use pip or any other Python package manager you prefer:

```bash
pip install pyfastcdc
```

For platforms without prebuilt wheels, a suitable build environment capable of compiling Python extension modules is required.
For example, on Debian, you might need to install `gcc` and `python3-dev` via `apt`

If the Cython extension fails to compile, the installation will fall back to a pure Python implementation,
which is significantly slower (around 0.01× or less in memory chunking speed)

## Usage

The basic usage is simple:

1. Construct a `FastCDC` instance with desired parameters
2. Call `FastCDC.cut_xxx()` function to chunk your data

Example:

```python
import hashlib
from pyfastcdc import FastCDC

for chunk in FastCDC(16384).cut_file('archive.tar'):
	print(chunk.offset, chunk.length, hashlib.sha256(chunk.data).hexdigest())
```

See [docstrings](pyfastcdc/__init__.pyi) of exported objects in the `pyfastcdc` module for more API details

Please only import members from `pyfastcdc` in your application code and avoid importing inner modules (e.g. `pyfastcdc.common`) directly.
Only public APIs inside the `pyfastcdc` module are guaranteed to be stable across releases

```python
from pyfastcdc import NormalizedChunking         # GOOD
from pyfastcdc.common import NormalizedChunking  # BAD, no API stability guarantee
```

## Performance

With the help of Cython, PyFastCDC can achieve near-native performance on chunking inputs

![benchmark](https://raw.githubusercontent.com/Fallen-Breath/pyfastcdc/refs/heads/master/benchmark.png)

Each test was run 10 times for averaging, achieving a maximum in-memory chunking speed of about 4.8GB/s

<details>

<summary>Benchmark details</summary>

FastCDC parameters:

- `avg_size`: Independent variable
- `min_size`: `avg_size` / 4 (default)
- `max_size`: `avg_size` * 4 (default)
- `normalized_chunking`: 1 (default)
- `seed`: 0 (default)

Test environment:

- PyFastCDC 0.2.0b1, precompiled wheel from Test PyPI, Cython 3.2.4
- Python 3.11.14 using docker image `python:3.11`
- Ryzen 7 6800H @ 4.55GHz, NVMe SSD, Debian 13.2

Test files:

- `rand_10G.bin`: 10GiB randomly generated binary data
- [`AlmaLinux-10.1-x86_64-dvd.iso`](https://repo.almalinux.org/almalinux/10/isos/x86_64/AlmaLinux-10.1-x86_64-dvd.iso): the DVD ISO image of AlmaLinux 10.1
- [`llvmorg-21.1.8.tar`](https://github.com/llvm/llvm-project/archive/refs/tags/llvmorg-21.1.8.tar.gz): gzip-unzipped LLVM 21.1.8 source code

Test command:

```bash
cd scripts
python benchmark.py --test-files rand_10G.bin AlmaLinux-10.1-x86_64-dvd.iso llvmorg-21.1.8.tar
```

</details>

## Difference from iscc/fastcdc-py

This project is inspired by [iscc/fastcdc-py](https://github.com/iscc/fastcdc-py), but differs in the following ways:

1. Based on nlfiedler/fastcdc-rs, using its FastCDC 2020 implementation aligned with the original paper, rather than the simplified ronomon implementation
2. Supports multiple types of input, including in-memory data buffers, regular file using mmap, and custom streaming input
3. Does not include any CLI tool. It provides only the core FastCDC functionality

## License

[MIT](LICENSE)

## Reference

Papers

- FastCDC 2016: [FastCDC: A Fast and Efficient Content-Defined Chunking Approach for Data Deduplication](https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf)
- FastCDC 2020: [The Design of Fast Content-Defined Chunking for Data Deduplication Based Storage Systems](https://ieeexplore.ieee.org/document/9055082)

Other FastCDC Implementations

- [HIT-HSSL/destor](https://github.com/HIT-HSSL/destor/blob/master/src/chunking/fascdc_chunking.c), the C implementation reference from the paper
- [nlfiedler/fastcdc-rs](https://github.com/nlfiedler/fastcdc-rs), where this implementation is based on
- [iscc/fastcdc-py](https://github.com/iscc/fastcdc-py), which provides an alternative FastCDC implementation based on [ronomon/deduplication](https://github.com/ronomon/deduplication)
