class Product:
	def __init__(self, stockcode, productImageUrl, volumetricWeight):
		self.Stockcode = stockcode
		self.ProductImageUrl = productImageUrl
		self.VolumetricWeight = volumetricWeight

	@staticmethod
	def Create(stockcode, productimageurl, volumetricweight):
		return Product(stockcode, productimageurl, volumetricweight)





