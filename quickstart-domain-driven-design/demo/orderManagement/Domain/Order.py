import orderManagement.Domain.Services.CostCalculatorService as CostCalculatorService
from orderManagement.Domain.Address import Address
from orderManagement.Domain.TransitLocation import TransitLocation
import datetime

class Order:
    def __init__(self, orderId, orderLines, customerId, totalCost, shippingCost, billingAddress, shippingAddress,
                 promotionCode, datePlaced, transitLocations,
                 costCalculatorService, productAvailabilityService, orderTrackingRepository):
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
        self._costCalculatorService = costCalculatorService
        self._productAvailabilityService = productAvailabilityService
        self._orderTrackingRepository = orderTrackingRepository

    @staticmethod
    def Create(orderLines, customerId, billingAddress, shippingAddress, promotionCode, datePlaced,
               costCalculatorService, productAvailabilityService, orderTrackingRepository):
        return Order(-1, orderLines, customerId, -1, -1, billingAddress, shippingAddress, promotionCode, datePlaced, None,
                     costCalculatorService, productAvailabilityService, orderTrackingRepository)

    @staticmethod
    def Create1(orderId, orderLines, customerId, totalCost, shippingCost,
               billingAddress, shippingAddress, promotionCode, datePlaced):
        return Order(orderId, orderLines, customerId, totalCost, shippingCost, billingAddress, shippingAddress,
                     promotionCode, datePlaced, None, None, None, None)

    def CalculateShippingCost(self):
        products = []
        for orderLine in self.OrderLines:
            products.append(orderLine.Product)
        self.ShippingCost = self._costCalculatorService.CalculateShippingPrice(products, self.ShippingAddress)

    def CalculateTotalCost(self):
        self.TotalCost = self._costCalculatorService.CalculateTotalPrice(self.OrderLines, self.PromotionCode)

    def CanPlaceOrder(self, expectedTotalCost, expectedShippingCost):
        # An order must have at least one line
        if len(self.OrderLines) ==0:
            return False
        # All products must be available to order
        for line in self.OrderLines:
            if not self._productAvailabilityService.CheckProductAvailability(line.Product.Stockcode, line.Quantity):
                return False
        # The calculated costs must match the expected ones
        self.CalculateShippingCost()
        self.CalculateTotalCost()
        if self.TotalCost != expectedTotalCost or self.ShippingCost != expectedShippingCost:
            return False

        # if all checks succeeded, return True
        return True

    def LoadTransitLocations(self):
        self.TransitLocations = self._orderTrackingRepository.GetTransitLocations(self.OrderId)

    def WithOrderTrackingRepository(self, orderTrackingRepository):
        self._orderTrackingRepository = orderTrackingRepository




