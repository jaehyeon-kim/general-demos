from abc import ABC, abstractmethod


class ICostCalculatorService(ABC):
    @abstractmethod
    def CalculateTotalPrice(self, order_lines, promotion_code):
        pass

    @abstractmethod
    def CalculateShippingPrice(self, products, shipping_address):
        pass
