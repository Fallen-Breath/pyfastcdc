import argparse
import hashlib
import struct
from pathlib import Path
from typing import List

MASKS = '''
[
	0,                   # padding
	0,                   # padding
	0,                   # padding
	0,                   # padding
	0,                   # padding
	0x0000000001804110,  # unused except for NC 3
	0x0000000001803110,  # 64B
	0x0000000018035100,  # 128B
	0x0000001800035300,  # 256B
	0x0000019000353000,  # 512B
	0x0000590003530000,  # 1KB
	0x0000d90003530000,  # 2KB
	0x0000d90103530000,  # 4KB
	0x0000d90303530000,  # 8KB
	0x0000d90313530000,  # 16KB
	0x0000d90f03530000,  # 32KB
	0x0000d90303537000,  # 64KB
	0x0000d90703537000,  # 128KB
	0x0000d90707537000,  # 256KB
	0x0000d91707537000,  # 512KB
	0x0000d91747537000,  # 1MB
	0x0000d91767537000,  # 2MB
	0x0000d93767537000,  # 4MB
	0x0000d93777537000,  # 8MB
	0x0000d93777577000,  # 16MB
	0x0000db3777577000,  # unused except for NC 3
]
'''.strip()

TEMPLATE_CY = '''
from libc.stdint cimport uint64_t

cdef uint64_t[26] MASKS = {{MASKS}}

cdef uint64_t[256] GEAR = [
{{GEAR}}
]

cdef uint64_t[256] GEAR_LS = [
{{GEAR_LS}}
]
'''.lstrip()

TEMPLATE_PY = '''
from typing import List, Final

MASKS: Final[List[int]] = {{MASKS}}

GEAR: Final[List[int]] = [
{{GEAR}}
]

GEAR_LS: Final[List[int]] = [
{{GEAR_LS}}
]
'''.lstrip()


def main():
	parser = argparse.ArgumentParser()
	parser.add_argument('--output', type=str, default=str(Path(__file__).parent.parent / 'fastcdc2020'))
	args = parser.parse_args()

	output_dir = Path(args.output)
	output_file_cy = output_dir / 'cy' / 'constants.pyx'
	output_file_py = output_dir / 'py' / 'constants.py'

	# https://github.com/HIT-HSSL/destor/blob/master/src/chunking/fascdc_chunking.c fastcdc_init
	table_len = 256
	seed_len = 64
	gear: List[str] = []
	gear_ls: List[str] = []
	for i in range(table_len):
		buf = bytes([i]) * seed_len
		digest = struct.unpack('>Q', hashlib.md5(buf).digest()[:8])[0]
		digest_ls = (digest << 1) & ((1 << 64) - 1)
		gear.append('0x' + hex(digest)[2:].upper().rjust(16, '0'))
		gear_ls.append('0x' + hex(digest_ls)[2:].upper().rjust(16, '0'))

	lines_gear: List[str] = []
	lines_gear_ls: List[str] = []
	item_per_line = 4
	assert len(gear) == len(gear_ls)
	assert len(gear) % item_per_line == 0
	for i in range(0, len(gear), item_per_line):
		lines_gear.append('\t' + ', '.join(gear[i:i + item_per_line]) + ',')
		lines_gear_ls.append('\t' + ', '.join(gear_ls[i:i + item_per_line]) + ',')

	def format_template(s: str) -> str:
		s = s.replace('{{MASKS}}', MASKS)
		s = s.replace('{{GEAR}}', '\n'.join(lines_gear))
		s = s.replace('{{GEAR_LS}}', '\n'.join(lines_gear_ls))
		return s

	with open(output_file_cy, 'w', encoding='utf8') as f:
		f.write(format_template(TEMPLATE_CY))
	with open(output_file_py, 'w', encoding='utf8') as f:
		f.write(format_template(TEMPLATE_PY))


if __name__ == '__main__':
	main()
