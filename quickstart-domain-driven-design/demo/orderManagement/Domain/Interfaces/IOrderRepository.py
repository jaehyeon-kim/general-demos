from abc import ABC, abstractmethod


class IOrderRepository(ABC):
    @abstractmethod
    def Store(self, order):
        pass

    @abstractmethod
    def Load(self, order_id):
        pass

    @abstractmethod
    def Search(self, customer_id):
        pass
