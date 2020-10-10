import logging

import pandas as pd
from sqlalchemy import create_engine

from .meta.singleton_meta import SingletonMeta
from .tse_scrapper import TSEScrapper


class TSE(metaclass=SingletonMeta):

    def __init__(self, connection_arguments="sqlite:///tse.db", preload=False):
        self.logger = logging.getLogger(__name__)
        self.engine = create_engine(connection_arguments)
        self.df_tickers_list = None
        self.tickers_history = {}
        tse_scrapper = TSEScrapper(connection_arguments)
        self.logger.info("Checking whether stocks list table exist or not")
        if self.engine.dialect.has_table(self.engine, "tickers_list"):
            self.logger.info("Loading tickers_list table as a dataframe")
            self.fetch_tickers_list()
        else:
            self.logger.info("tickers_list table doesn't exits.")
            tse_scrapper.get_tickers_list()
            tse_scrapper.save_tickers_list()

        self.logger.info("Checking stocks history tables' existence")
        for i in self.df_tickers_list["id"]:
            if self.engine.dialect.has_table(self.engine, i):
                continue
            else:
                self.logger.info("Stocks history tables' don't exist")
                ticker_history = tse_scrapper.get_ticker_history(i)
                tse_scrapper.save_ticker_history(ticker_history)
                break

        if preload:
            self.fetch_all_tickers_history()

        self.logger.info("Checking whether sectors list table exist or not")
        if self.engine.dialect.has_table(self.engine, "sectors_list"):
            self.logger.info("Loading sectors list table as a dataframe")
            self.fetch_sectors_list()
        else:
            self.logger.info("Sectors list table doesn't exits.")
            tse_scrapper.get_sectors_list()
            tse_scrapper.save_sectors_list()

    def fetch_tickers_list(self):
        self.logger.info("Fetching df_tickers_list from database...")
        self.df_tickers_list = pd.read_sql_table(table_name="tickers_list", con=self.engine)

    def fetch_ticker_history(self, ticker):
        self.logger.info("Fetching ticker history for ticker = {} from database...".format(ticker))
        ticker_id = self.get_id_by_ticker(ticker)
        if ticker_id not in self.tickers_history:
            ticker_history = pd.read_sql_table(table_name=ticker_id, con=self.engine,
                                               parse_dates=["<DTYYYYMMDD>"])
            # Caching ticker_history
            self.tickers_history[ticker_id] = ticker_history
            return ticker_history
        else:
            return self.tickers_history[ticker_id]

    def fetch_all_tickers_history(self):
        self.logger.info("Fetching tickers history from database...")
        for i in self.df_tickers_list["id"]:
            self.tickers_history[i] = pd.read_sql_table(table_name=i, con=self.engine,
                                                        parse_dates=["<DTYYYYMMDD>"])

    def fetch_sectors_list(self):
        self.logger.info("Fetching df_sectors_list from database...")
        self.df_sectors_list = pd.read_sql_table(table_name="sectors_list", con=self.engine)

    def get_id_by_ticker(self, ticker):
        return self.tickers[self.tickers.ticker == ticker].id.values[0]

    @property
    def tickers(self):
        return self.df_tickers_list

    @property
    def sectors(self):
        return self.df_sectors_list
