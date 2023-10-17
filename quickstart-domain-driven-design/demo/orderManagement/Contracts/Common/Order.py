class Order:
    def __init__(self, orderId, orderLines, customerId, totalCost, shippingCost, billingAddress, shippingAddress,
             promotionCode, datePlaced, transitLocations):

        self.OrderId = orderId
        self.OrderLines = orderLines
        self.CustomerId = customerId
        self.TotalCost = totalCost
        self.ShippingCost = shippingCost
        self.BillingAddress = billingAddress
        self.ShippingAddress = shippingAddress
        self.PromotionCode = promotionCode
        self.DatePlaced = datePlaced
        self.TransitLocations = transitLocations

    @staticmethod
    def Create(orderId, orderLines, customerId, totalCost, shippingCost, billingAddress, shippingAddress,
            promotionCode, datePlaced):
        return Order(orderId, orderLines, customerId, totalCost, shippingCost, billingAddress, shippingAddress,
                     promotionCode, datePlaced, None)

    def asdict(self):
        orderLinesDict = [x.asdict() for x in self.OrderLines]
        if self.TransitLocations is None:
            transitLocationsDict = ""
        else:
            transitLocationsDict = [x.asdict() for x in self.TransitLocations]
        return {'OrderId': self.OrderId,
                'OrderLines': orderLinesDict,
                'CustomerId': self.CustomerId,
                'TotalCost': self.TotalCost, 'ShippingCost': self.ShippingCost,
                'BillingAddress': self.BillingAddress.asdict(),
                'ShippingAddress': self.ShippingAddress.asdict(),
                'PromotionCode': self.PromotionCode,'DatePlaced': self.DatePlaced,
                'TransitLocations': transitLocationsDict
                }

