import random
from pathlib import Path
from typing import Type, Union

import pytest

from pyfastcdc import FastCDC as FastCDC_cy
from pyfastcdc.py import FastCDC as FastCDC_py


@pytest.fixture(params=['cy', 'py'])
def fastcdc_impl(request) -> Type:
	if request.param == 'cy':
		return FastCDC_cy
	else:
		return FastCDC_py


@pytest.fixture
def fastcdc_instance(fastcdc_impl) -> Union[FastCDC_cy, FastCDC_py]:
	return fastcdc_impl(avg_size=16384)


SEKIEN_AKASHITA_PATH = Path(__file__).parent / 'fixtures' / 'SekienAkashita.jpg'


@pytest.fixture
def sekien_akashita_path() -> Path:
	return SEKIEN_AKASHITA_PATH


@pytest.fixture
def sekien_akashita_bytes() -> bytes:
	return SEKIEN_AKASHITA_PATH.read_bytes()


def __create_random_data(size: int) -> bytes:
	rnd = random.Random(0)
	return bytes(rnd.getrandbits(8) for _ in range(size))


@pytest.fixture
def random_data_100k() -> bytes:
	return __create_random_data(100)


@pytest.fixture
def random_data_1m() -> bytes:
	return __create_random_data(1024 * 1024)


@pytest.fixture
def random_data_10m() -> bytes:
	return __create_random_data(10 * 1024 * 1024)
