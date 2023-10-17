from orderManagement.Domain.Interfaces.ICostCalculatorService import ICostCalculatorService


class CostCalculatorService(ICostCalculatorService):
	def __init__(self):
		return

	def CalculateShippingPrice(self, products, shipping_address):
		return 50

	def CalculateTotalPrice(self, order_lines,  promotion_code):
		if len(order_lines) == 0:
			return 0
		sum_price = 0
		for line in order_lines:
			sum_price = sum_price + line.UnitPrice * line.Quantity
		return sum_price




