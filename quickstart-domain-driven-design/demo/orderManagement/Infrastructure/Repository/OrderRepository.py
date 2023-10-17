import datetime
from orderManagement.Domain.Address import Address
from orderManagement.Domain.Interfaces.IOrderRepository import IOrderRepository
from orderManagement.Domain.Order import Order
from orderManagement.Domain.Product import Product
from orderManagement.Domain.OrderLine import OrderLine


class OrderRepository(IOrderRepository):

	def Load(self, order_id):
		product1 = Product.Create(504421, "/image/504421/a.jpg", 392)
		product2 = Product.Create(23151, "/image/23151/ce.jpg", 50)
		product3 = Product.Create(40833, "/image/40833/gev.jpg", 22)
		order_line1 = OrderLine.Create(product1, 4, 40)
		order_line2 = OrderLine.Create(product2, 2, 10)
		order_line3 = OrderLine.Create(product3, 3, 14)
		order_lines = [order_line1, order_line2, order_line3]
		address1 = Address.Create("address1", "address2", "country")
		address2 = Address.Create("address1", "address2", "country")
		order = Order.Create1(101, order_lines, 5, 408, 89, address1, address2, "FIRSTBUY", datetime.datetime.now())
		return order

	def Search(self, customer_id):
		product1 = Product.Create(504421,"/image/504421/a.jpg", 392)
		product2 = Product.Create(23151, "/image/23151/ce.jpg", 50)
		product3 = Product.Create(40833, "/image/40833/gev.jpg", 22)
		order_line1 = OrderLine.Create(product1, 4, 40)
		order_line2 = OrderLine.Create(product2, 2, 10)
		order_line3 = OrderLine.Create(product3, 3, 14)
		order_lines1 = [order_line1, order_line2, order_line3]

		product4 = Product.Create(504421, "/image/504311/a4.jpg", 34)
		product5 = Product.Create(23151, "/image/23333/cf.jpg", 16)
		order_line3 = OrderLine.Create(product4, 12, 35)
		order_line4 = OrderLine.Create(product5, 25, 13)
		order_lines2 = [order_line3, order_line4]

		address1 = Address.Create("address1", "address2", "country")
		address2 = Address.Create("address1", "address2", "country")
		order1 = Order(101, order_lines1, 5, 408, 89, address1, address2, "FIRSTBUY", datetime.datetime.now(), None, None, None, None)
		order2 = Order(156, order_lines2, 5, 59, 39, address1, address2, "", datetime.datetime.now(), None, None, None, None)
		orders = [order1, order2]
		return orders

	def Store(self, order):
		return 503



