# pyfastcdc

FastCDC 2020 implementation in Python, with Cython acceleration

**Still under development**

## Installation

TODO

```bash
pip install pyfastcdc
```

## Usage

TODO

```python
import hashlib
from pyfastcdc import FastCDC

for chunk in FastCDC(16384).cut_file('archive.tar'):
	print(chunk.offset, chunk.length, hashlib.sha256(chunk.data).hexdigest())
```

## Performance

![benchmark](benchmark.png)

Test environment:

- Python 3.11.14 (docker image `python:3.11`)
- Ryzen 7 6800H @ 4.55GHz, NVMe SSD, Debian 13.2

Each test was run five times for averaging, achieving a maximum in-memory chunking speed of about 4.8 GB/s

Test files:

- `rand_10G.bin`: 10GB randomly generated data
- [`AlmaLinux-10.1-x86_64-dvd.iso`](https://repo.almalinux.org/almalinux/10/isos/x86_64/AlmaLinux-10.1-x86_64-dvd.iso): 
- [`llvmorg-21.1.8.tar`](https://github.com/llvm/llvm-project/archive/refs/tags/llvmorg-21.1.8.tar.gz): gzip-unzipped LLVM 21.1.8 source code

Test command:

```bash
python ./scripts/benchmark.py --test-files rand_10G.bin AlmaLinux-10.1-x86_64-dvd.iso llvmorg-21.1.8.tar
```

## License

[MIT](LICENSE)

## Reference

Papers

- FastCDC 2016: [FastCDC: A Fast and Efficient Content-Defined Chunking Approach for Data Deduplication](https://www.usenix.org/system/files/conference/atc16/atc16-paper-xia.pdf)
- FastCDC 2020: [The Design of Fast Content-Defined Chunking for Data Deduplication Based Storage Systems](https://ieeexplore.ieee.org/document/9055082)

Other Implementations

- [nlfiedler/fastcdc-rs](https://github.com/nlfiedler/fastcdc-rs), where this implementation is based on
- [iscc/fastcdc-py](https://github.com/iscc/fastcdc-py), which provides an alternative FastCDC implementation based on [ronomon/deduplication](https://github.com/ronomon/deduplication)
