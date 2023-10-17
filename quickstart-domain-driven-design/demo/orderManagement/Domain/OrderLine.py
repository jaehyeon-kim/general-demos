class OrderLine:
    def __init__(self, product, quantity, unitprice):
        self.Product = product
        self.Quantity = quantity
        self.UnitPrice = unitprice
    @staticmethod
    def Create(product, quantity, unitprice):
        return  OrderLine(product, quantity, unitprice)

