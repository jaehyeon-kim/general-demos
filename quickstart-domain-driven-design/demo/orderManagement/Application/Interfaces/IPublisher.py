from abc import ABC, abstractmethod


class IPublisher(ABC):
    @abstractmethod
    def Publish(self, o):
        pass
