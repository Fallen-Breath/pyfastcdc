import hashlib
import io
import random
from io import BytesIO
from pathlib import Path
from typing import Dict, Tuple, List, NamedTuple

import pytest

from pyfastcdc.common import NormalizedChunking
from pyfastcdc.cy import FastCDC as FastCDC_cy
from pyfastcdc.py import FastCDC as FastCDC_py
from tests.utils import FastCDCType


class TestSekienAkashitaImage:
	class Param(NamedTuple):
		avg_size: int
		seed: int = 0
		nc: NormalizedChunking = 1
	# Param -> [(gear_hash, length), ...]
	EXPECTED_RESULT: Dict[Param, List[Tuple[int, int]]] = {}

	@pytest.mark.parametrize('cut_func', ['buf', 'stream'])
	@pytest.mark.parametrize('case_param', EXPECTED_RESULT.keys())
	def test_sekien_akashita(self, fastcdc_impl: FastCDCType, sekien_akashita_bytes: bytes, case_param: Param, cut_func: str):
		expected = self.EXPECTED_RESULT[case_param]

		cdc = fastcdc_impl(avg_size=case_param.avg_size, seed=case_param.seed, normalized_chunking=case_param.nc)

		if cut_func == 'buf':
			chunk_gen = cdc.cut_stream(BytesIO(sekien_akashita_bytes))
		elif cut_func == 'stream':
			chunk_gen = cdc.cut_buf(sekien_akashita_bytes)
		else:
			raise ValueError(cut_func)

		h = hashlib.sha256()
		chunk_cnt = 0
		for i, chunk in enumerate(chunk_gen):
			assert i < len(expected)
			assert chunk.gear_hash == expected[i][0]
			assert chunk.length == expected[i][1]
			h.update(chunk.data)
			chunk_cnt += 1
		assert chunk_cnt == len(expected)
		assert h.hexdigest() == hashlib.sha256(sekien_akashita_bytes).hexdigest()


class TestPyCyConsistency:
	DATA_SIZES = [
		*range(100),
		*[i * 100 for i in range(1, 10)],
		*[i * 1024 for i in range(1, 10)],
		*[i * 10 * 1024 for i in range(1, 5)],
		*[i * 100 * 1024 for i in range(1, 4)],
		*[i * 1024 * 1024 for i in range(1, 3)],
	]
	AVG_SIZES = [1024, 4096, 8192, 12345, 16384, 65536]

	@pytest.fixture(scope="class")
	def random_data_by_size(self) -> Dict[int, bytes]:
		data_cache = {}
		for size in self.DATA_SIZES:
			rnd = random.Random(size)
			data_cache[size] = bytes(rnd.getrandbits(8) for _ in range(size))
		return data_cache

	@pytest.mark.parametrize('avg_size', AVG_SIZES)
	@pytest.mark.parametrize('data_size', DATA_SIZES)
	@pytest.mark.parametrize('normalized_chunking', [0, 1, 2, 3])
	@pytest.mark.parametrize('seed', [0, 1])
	def test_py_cy_consistency(self, avg_size: int, data_size: int, normalized_chunking: NormalizedChunking, seed: int, random_data_by_size: Dict[int, bytes]):
		data = random_data_by_size[data_size]

		cdc_py = FastCDC_py(avg_size=avg_size, normalized_chunking=normalized_chunking, seed=seed)
		cdc_cy = FastCDC_cy(avg_size=avg_size, normalized_chunking=normalized_chunking, seed=seed)
		assert cdc_py.avg_size == cdc_cy.avg_size
		assert cdc_py.min_size == cdc_cy.min_size
		assert cdc_py.max_size == cdc_cy.max_size

		chunks_py = list(cdc_py.cut_buf(data))
		chunks_cy = list(cdc_cy.cut_buf(data))

		assert len(chunks_py) == len(chunks_cy)

		for py_chunk, cy_chunk in zip(chunks_py, chunks_cy):
			assert py_chunk.offset == cy_chunk.offset
			assert py_chunk.length == cy_chunk.length
			assert py_chunk.gear_hash == cy_chunk.gear_hash
			assert bytes(py_chunk.data) == bytes(cy_chunk.data)

		reconstructed_py = b''.join(bytes(chunk.data) for chunk in chunks_py)
		reconstructed_cy = b''.join(bytes(chunk.data) for chunk in chunks_cy)
		assert reconstructed_py == data
		assert reconstructed_cy == data


class TestCutMethods:
	def test_chunk_properties(self, fastcdc_instance, random_data_1m: bytes):
		prev_offset = None
		prev_size = None
		for chunk in fastcdc_instance.cut_buf(random_data_1m):
			assert isinstance(chunk.gear_hash, int)
			assert isinstance(chunk.offset, int)
			assert isinstance(chunk.length, int)
			assert isinstance(chunk.data, memoryview)

			assert len(chunk.data) == chunk.length

			if prev_offset is not None:
				assert chunk.offset == prev_offset + prev_size
			prev_offset = chunk.offset
			prev_size = chunk.length

	def test_cut_buf_vs_cut_file_consistency(self, fastcdc_impl: FastCDCType, random_data_1m: bytes, tmp_path: Path):
		cdc = fastcdc_impl(avg_size=8192)
		chunks_memory = list(cdc.cut_buf(random_data_1m))

		temp_file = tmp_path / 'test.bin'
		temp_file.write_bytes(random_data_1m)

		chunk_cnt = 0
		for i, chunk in enumerate(cdc.cut_file(temp_file)):
			chunk_cnt += 1
			assert chunk_cnt <= len(chunks_memory)
			assert chunk.offset == chunks_memory[i].offset
			assert chunk.length == chunks_memory[i].length
			assert chunk.gear_hash == chunks_memory[i].gear_hash
			assert bytes(chunk.data) == bytes(chunks_memory[i].data)
		assert len(chunks_memory) == chunk_cnt

	def test_cut_buf_vs_cut_stream_consistency_read(self, fastcdc_impl: FastCDCType, random_data_1m: bytes):
		cdc = fastcdc_impl(avg_size=8192)
		chunks_memory = list(cdc.cut_buf(random_data_1m))
		bytes_io = io.BytesIO(random_data_1m)

		class MyStream:
			def read(self, n: int) -> bytes:
				return bytes_io.read(n)

		chunk_cnt = 0
		for i, chunk in enumerate(cdc.cut_stream(MyStream())):
			chunk_cnt += 1
			assert chunk_cnt <= len(chunks_memory)
			assert chunk.offset == chunks_memory[i].offset
			assert chunk.length == chunks_memory[i].length
			assert chunk.gear_hash == chunks_memory[i].gear_hash
			assert bytes(chunk.data) == bytes(chunks_memory[i].data)
		assert len(chunks_memory) == chunk_cnt

	def test_cut_buf_vs_cut_stream_consistency_readinto(self, fastcdc_impl: FastCDCType, random_data_1m: bytes):
		cdc = fastcdc_impl(avg_size=8192)
		chunks_memory = list(cdc.cut_buf(random_data_1m))
		bytes_io = io.BytesIO(random_data_1m)

		class MyStream:
			def readinto(self, buf) -> int:
				return bytes_io.readinto(buf)

		chunk_cnt = 0
		for i, chunk in enumerate(cdc.cut_stream(MyStream())):
			chunk_cnt += 1
			assert chunk_cnt <= len(chunks_memory)
			assert chunk.offset == chunks_memory[i].offset
			assert chunk.length == chunks_memory[i].length
			assert chunk.gear_hash == chunks_memory[i].gear_hash
			assert bytes(chunk.data) == bytes(chunks_memory[i].data)
		assert len(chunks_memory) == chunk_cnt


class TestSeed:
	def test_different_seeds_produce_different_chunks(self, random_data_1m: bytes):
		cdc1 = FastCDC_cy(avg_size=8192, seed=1)
		cdc2 = FastCDC_cy(avg_size=8192, seed=2)

		chunks1 = list(cdc1.cut_buf(random_data_1m))
		chunks2 = list(cdc2.cut_buf(random_data_1m))

		hashes1 = [chunk.gear_hash for chunk in chunks1]
		hashes2 = [chunk.gear_hash for chunk in chunks2]

		assert hashes1 != hashes2 or len(chunks1) == 0


class TestChunkProperties:
	def test_chunk_data_integrity(self, fastcdc_instance, random_data_1m: bytes):
		data_list = []
		for chunk in fastcdc_instance.cut_buf(random_data_1m):
			data_list.append(bytes(chunk.data))
		assert b''.join(data_list) == random_data_1m

	def test_chunk_size_constraints(self, fastcdc_impl: FastCDCType, random_data_1m: bytes):
		avg_size = 16384
		min_size = avg_size // 4  # 4096
		max_size = avg_size * 4  # 65536

		cdc = fastcdc_impl(avg_size=avg_size)
		chunks = list(cdc.cut_buf(random_data_1m))

		for chunk in chunks:
			if chunk != chunks[-1]:
				assert min_size <= chunk.length <= max_size



def __build_test_data():
	cases = TestSekienAkashitaImage.EXPECTED_RESULT
	key = TestSekienAkashitaImage.Param
	cases[key(16384, 0)] = [
		(17968276318003433923, 21325),
		(8197189939299398838, 17140),
		(13019990849178155730, 28084),
		(4509236223063678303, 18217),
		(2504464741100432583, 24700),
	]
	cases[key(16384, 555, 0)] = [
		(11912081672558759016, 65536),
		(4429013657701329409, 15481),
		(4617353815122521848, 25471),
		(0, 2978),
	]
	cases[key(16384, 666, 1)] = [
        (9312357714466240148, 10605),
        (226910853333574584, 55745),
        (12271755243986371352, 11346),
        (14153975939352546047, 5883),
        (5890158701071314778, 11586),
        (8981594897574481255, 14301),
	]
	cases[key(16384, 777, 2)] = [
		(5756766315125948951, 35163),
		(17831515366867633926, 16422),
		(3216207887329694204, 17470),
		(1561632489531769228, 10294),
		(10369060306646295092, 18786),
		(14655496504067886486, 11331),
	]
	cases[key(16384, 888, 3)] = [
		(3158156329559295338, 17507),
		(5701878486563355731, 18589),
		(4529013630687100674, 17996),
		(3219799010358125664, 17281),
		(9266722211030107244, 15987),
		(7683217499559119532, 19292),
		(0, 2814),
	]
	cases[key(32768, 0)] = [
		(15733367461443853673, 66549),
		(6321136627705800457, 42917),
	]
	cases[key(65536, 0)] = [
		(2504464741100432583, 109466),
	]
	cases[key(1000, 0)] = [
		(2890614758789387787, 359),
		(4044080065227531274, 1501),
		(9649269133818233035, 1087),
		(2854337592871656094, 589),
		(4465407710123297832, 1307),
		(13038202513543136492, 944),
		(221561130519947581, 847),
		(393511086818658384, 268),
		(8088160130455062548, 1492),
		(9649269133818233035, 1087),
		(2854337592871656094, 589),
		(4465407710123297832, 1307),
		(6977244030422521668, 951),
		(14622491748227350624, 497),
		(3757277802856017243, 4000),
		(14582375164208481996, 525),
		(15870580120602364088, 1122),
		(17583755766661134474, 714),
		(11818998251676538346, 267),
		(4691013873021002775, 1135),
		(17968276318003433923, 737),
		(7923522120571464808, 1245),
		(1606623465266480327, 1033),
		(14432714177272874680, 274),
		(9475721149199180143, 627),
		(13127205120246874280, 1226),
		(4571237624765828492, 840),
		(13107907174429668598, 779),
		(8865631699531232422, 256),
		(12688194934178599784, 521),
		(6160462813037351348, 1038),
		(4590271154362536204, 1774),
		(11432121548796343504, 674),
		(12472017965383948508, 1114),
		(11136788103843283336, 1086),
		(14747293179156137189, 1135),
		(4155943948347508955, 1575),
		(13104072099671895560, 739),
		(12639643500133003296, 978),
		(6291550675693738136, 1014),
		(12146666992319875138, 1754),
		(867790609525255823, 823),
		(7795831017548588040, 1197),
		(6071878725246451920, 1128),
		(9006935664263516606, 1230),
		(10730962063819841566, 1243),
		(13986533877452114082, 311),
		(11213593662057882791, 1563),
		(8419216051635751960, 2535),
		(17824018122213720558, 649),
		(7691821336090384658, 1058),
		(15627606194869835864, 1006),
		(6161241554519610597, 937),
		(6941914075316668026, 399),
		(1182793066038454380, 733),
		(12071179740026769918, 1016),
		(4347472036401076612, 1016),
		(2230176819808087171, 1437),
		(13643730949769321254, 970),
		(11037552763304977452, 1497),
		(5378144661922417606, 887),
		(1077206070806454392, 474),
		(15663206445926121842, 1020),
		(10340633799421599932, 1189),
		(10670100924980276327, 1005),
		(17729339453970921664, 314),
		(16991238872615146150, 790),
		(6422874623524250261, 479),
		(12672356704810770552, 442),
		(11984928221549859605, 395),
		(2097593348099185500, 491),
		(14878246610844549137, 519),
		(5632890640695056296, 698),
		(234053196780936967, 485),
		(6405742965701021702, 1168),
		(10119588664293507636, 432),
		(3677460465457318012, 1892),
		(2929707338101168456, 1200),
		(14759779960424595626, 492),
		(8961005490888315294, 1076),
		(7045320843275616092, 546),
		(14573796348018082636, 613),
		(5720125013745730749, 1003),
		(9959758967528298632, 1240),
		(5498194506803611575, 655),
		(14533574346244646128, 685),
		(2286118599908196562, 1132),
		(12141987400064210298, 706),
		(11697621655555146678, 262),
		(4509236223063678303, 721),
		(9379039799252626497, 1991),
		(14161603951508589000, 1058),
		(3798969362984554894, 1462),
		(2208804812521734162, 1387),
		(743230986619455593, 1117),
		(10460176299449652894, 365),
		(16295441167671743576, 426),
		(12602762712196340732, 256),
		(6892378923297184865, 815),
		(2803535595082170474, 1100),
		(12936956031161931870, 328),
		(18083927691172312036, 703),
		(17523284212269230251, 1225),
		(12395604404862019884, 384),
		(15603180924257441853, 991),
		(10309388002691473494, 438),
		(11759658943204820007, 1203),
		(38352910123852128, 298),
		(839329000209336340, 1032),
		(17995146755519841483, 1323),
		(11992699556916938274, 265),
		(14564955722602338370, 1204),
		(9433076059681066661, 269),
		(4983236323095609658, 1054),
		(3633433896241630446, 1351),
		(1233903317720007796, 1725),
		(15335462755167671769, 633),
		(11585985329590265609, 297),
	]
	cases[key(4096, 123)] = [
		(17640604251071308688, 5626),
		(17640604251071308688, 6534),
		(1164779717113740270, 5375),
		(1630317330713120546, 2204),
		(15925855439796383684, 7352),
		(14038325076263464756, 4459),
		(10372641652373760866, 2757),
		(6632718265737316290, 4345),
		(13547989595704043725, 13255),
		(5085413808508269073, 6871),
		(2088831145520463002, 1867),
		(17282638805110740002, 7908),
		(10392127899000403174, 4664),
		(12682705380171701888, 3262),
		(14163264652508939164, 4595),
		(11317546923098927019, 10371),
		(5442637979354971713, 5365),
		(918741369407724947, 3523),
		(11012502959421423036, 2886),
		(16443206251033749990, 4470),
		(18389377892984539132, 1777),
	]
	cases[key(3333, 456)] = [
		(10244635572917292382, 5352),
		(9863741455868196496, 3576),
		(7958227187487219312, 11348),
		(1568981245086988673, 4093),
		(6960773682648885580, 3339),
		(774338163733218710, 3524),
		(8689733537919960031, 1741),
		(227474149701075941, 1347),
		(10343926834217379667, 2919),
		(16005375745858244513, 3381),
		(15597163692867778936, 2596),
		(10144991532768329190, 1426),
		(15692309460993287066, 2746),
		(12579192060780867954, 876),
		(15819814174051610080, 4398),
		(9470794240134493114, 4224),
		(1527989898970725766, 4315),
		(17216215298576800968, 2680),
		(2745509010730917144, 3990),
		(11949739281024563184, 2157),
		(8728826904842968300, 4092),
		(11802567455667178519, 3547),
		(16911338182963405550, 2591),
		(5388599717664141562, 849),
		(3598944074651431428, 4468),
		(14136883845736820776, 6320),
		(16089431095887575320, 2393),
		(932034647479080538, 4950),
		(1690856361285973629, 1591),
		(1258510376350532739, 1315),
		(12835826922199546104, 1612),
		(1125375003867751948, 1729),
		(6560578492574969505, 3981),
	]
	cases[key(7696, 789)] = [
		(3635570818360047716, 7841),
		(3361977302863881087, 12645),
		(12589329716764299352, 7886),
		(9249278391062916766, 8282),
		(3136269063085371990, 2172),
		(2076160877505623160, 3592),
		(3919332495791756340, 7766),
		(10241186054488990483, 5785),
		(2179260573026396498, 5640),
		(9176127053874656948, 4879),
		(16197834508891865920, 8870),
		(9292621146426491660, 8566),
		(1003212569724862660, 6473),
		(17552258689478972773, 3837),
		(11098559764229607138, 8020),
		(14721988269191742682, 2207),
		(13044652610110219114, 5005),
	]


__build_test_data()
