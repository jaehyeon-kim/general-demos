from abc import ABC, abstractmethod


class IProductAvailabilityService(ABC):
    @abstractmethod
    def CheckProductAvailability(self, stock_code, quantity):
        pass
