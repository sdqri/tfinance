import logging

import pandas as pd
from sqlalchemy import create_engine
from .meta.singleton_meta import SingletonMeta
from .tse_scrapper import TSEScrapper

class TSE(metaclass=SingletonMeta):

    def __init__(self, connection_arguments="sqlite:///tse.db", preload=False):
        self.logger = logging.getLogger(__name__)
        self.engine = create_engine(connection_arguments)
        tse_scrapper = TSEScrapper(connection_arguments)
        self.logger.info("Checking whether stocks list table exist or not")
        if self.engine.dialect.has_table(self.engine, "tickers_list"):
            self.logger.info("Loading tickers_list table as a dataframe")
            self.fetch_stocks_list()
        else:
            self.logger.info("tickers_list table doesn't exits.")
            tse_scrapper.get_tickers_list()
            tse_scrapper.save_tickers_list()

        self.logger.info("Checking stocks history tables' existence")
        for i in self.df_stocks_list["id"]:
            if self.engine.dialect.has_table(self.engine, i):
                continue
            else:
                self.logger.info("Stocks history tables' don't exist")
                tse_scrapper.get_history()
                tse_scrapper.save_history()
                break

        if (preload):
            self.fetch_history()

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
        self.df_tickers_list = pd.read_sql_table(table_name="stocks_list", con=self.engine)

    def fetch_tickers_history(self):
        self.tickers = {}
        self.logger.info("Fetching tickers history from database...")
        for i in self.df_tickers_list["id"]:
            self.tickers[i] = pd.read_sql_table(table_name=i, con=self.engine,
                                                    parse_dates=["<DTYYYYMMDD>"], chunksize=500)

    def fetch_history(self, id):
        self.logger.info("Fetching history from database...")
        history = pd.read_sql_table(table_name=id, con=self.engine,
                                                    parse_dates=["<DTYYYYMMDD>"], chunksize=500)
        return history

    def fetch_sectors_list(self):
        self.logger.info("Fetching df_sectors_list from database...")
        self.df_sectors_list = pd.read_sql_table(table_name="sectors_list", con=self.engine)

    @property
    def tickers(self):
        return self.df_tickers_list

    @property
    def sectors(self):
        return self.df_sectors_list