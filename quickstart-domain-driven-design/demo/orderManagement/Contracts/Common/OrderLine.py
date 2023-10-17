class OrderLine:
    def __init__(self, product, quantity, unitPrice):
        self.Product = product
        self.Quantity = quantity
        self.UnitPrice = unitPrice

    @staticmethod
    def Create(product, quantity, unitprice):
        return  OrderLine(product, quantity, unitprice)

    def asdict(self):
        return {'Product': self.Product.asdict(), 'Quantity': self.Quantity, 'UnitPrice': self.UnitPrice}


