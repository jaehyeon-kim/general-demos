class Address:
	def __init__(self, addressline1, addressline2, country):
		self.AddressLine1 = addressline1
		self.AddressLine2 = addressline2
		self.Country = country

	@staticmethod
	def Create(addressline1, addressline2, country):
		return Address(addressline1, addressline2, country)

	# for making JSON serializable, convert to dict
	def asdict(self):
		return {'AddressLine1': self.AddressLine1, 'AddressLine2': self.AddressLine2, 'Country': self.Country}





