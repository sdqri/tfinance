from abc import ABC, abstractmethod

class BaseScrapper(ABC):

    @abstractmethod
    def update(self):
        pass

    @abstractmethod
    def update_tickers(self):
        pass

    @abstractmethod
    def update_history(self):
        pass

    @abstractmethod
    def update_sectors(self):
        pass

