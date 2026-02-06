__version__ = '0.0.1'

try:
	from fastcdc2020.cy.fastcdc import FastCDC2020 as FastCDC2020
except ImportError:
	from fastcdc2020.py.fastcdc import FastCDC2020 as FastCDC2020
