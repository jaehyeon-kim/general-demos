from orderManagement.Domain.Interfaces.IProductAvailabilityService import IProductAvailabilityService


class ProductAvailabilityService(IProductAvailabilityService):
    def __init__(self):
        return

    def CheckProductAvailability(self, stock_code, quantity):
        return True


