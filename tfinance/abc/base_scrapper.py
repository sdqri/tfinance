from abc import ABC, abstractmethod

class BaseScrapper(ABC):

    @abstractmethod
    def update(self):
        pass

    @abstractmethod
    def update_tickers(self):
        self.logger.info("Checking whether stocks list table exist or not")
        if self.engine.dialect.has_table(self.engine, "tickers"):
            self.logger.info("Loading tickers_list table as a dataframe")
            self.fetch_tickers_list()
        else:
            self.logger.info("tickers_list table doesn't exits.")
            self.get_tickers_list()
            self.save_tickers_list()

    @abstractmethod
    def update_history(self):
        self.logger.info("Checking stocks history tables' existence")
        for i in self.df_tickers["id"]:
            if self.engine.dialect.has_table(self.engine, i):
                continue
            else:
                self.logger.info("Stocks history tables' don't exist")
                ticker_history = self.get_ticker_history(i)
                self.save_ticker_history(ticker_history)
                break

    @abstractmethod
    def update_sectors(self):
        self.logger.info("Checking whether sectors list table exist or not")
        if self.engine.dialect.has_table(self.engine, "sectors_list"):
            self.logger.info("Loading sectors list table as a dataframe")
            self.fetch_sectors()
        else:
            self.logger.info("Sectors list table doesn't exits.")
            self.get_sectors_list()
            self.save_sectors_list()

