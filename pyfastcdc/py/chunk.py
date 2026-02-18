class Chunk:
	__slots__ = ('offset', 'length', 'data', 'gear_hash')

	def __init__(self, offset: int, length: int, data: memoryview, gear_hash: int):
		self.offset = offset
		self.length = length
		self.data = data
		self.gear_hash = gear_hash

	def __repr__(self) -> str:
		return f'<{self.__class__.__name__} offset={self.offset} length={self.length} gear_hash={self.gear_hash}>'
