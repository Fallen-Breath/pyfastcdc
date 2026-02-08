from typing import Union, Type

from pyfastcdc.cy import FastCDC as FastCDC_cy
from pyfastcdc.py import FastCDC as FastCDC_py

FastCDCType = Union[Type[FastCDC_cy], Type[FastCDC_py]]
