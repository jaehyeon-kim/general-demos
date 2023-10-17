from abc import ABC, abstractmethod


class IOrderService(ABC):
    @abstractmethod
    def PlaceOrder(self, request):
        pass

    @abstractmethod
    def GetOrderHistory(self, customer_id):
        pass

    @abstractmethod
    def GetOrderTrackingInfo(self, order_id):
        pass
