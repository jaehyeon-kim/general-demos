from abc import ABC, abstractmethod


class IOrderTrackingRepository(ABC):
    @abstractmethod
    def GetTransitLocations(self, order_id):
        pass
