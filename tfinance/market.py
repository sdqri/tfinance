import logging

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from . import models
from .models import TickerModel, SectorModel
from .meta.singleton_meta import SingletonMeta


from .tse_scrapper import TSEScrapper


class Market(metaclass=SingletonMeta):

    def __init__(self, connection_arguments="sqlite:///tse.db", Scrapper=TSEScrapper):
        self.logger = logging.getLogger(__name__)
        # Creating database session
        self.engine = create_engine(connection_arguments)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
        # Calling Scrapper
        self.scrapper = Scrapper(self.session)
        self.scrapper.update()
        # Initializing instance attributes
        self.__tickers = self.fetch_tickers()
        self.__sectors = self.fetch_sectors()

    def fetch_tickers(self, **kwargs):
        self.logger.info("Fetching df_tickers_list from database...")
        sql = self.session.query(TickerModel).statement
        tickers = pd.read_sql(sql=sql, con=self.session.bind)
        return tickers

    def fetch_tickers_filter_by(self, **kwargs):
        self.logger.info("Fetching tickers from database...")
        sql = self.session.query(TickerModel).filter_by(**kwargs).statement
        tickers = pd.read_sql(sql=sql, con=self.session.bind)
        return tickers


    def fetch_sectors(self):
        self.logger.info("Fetching df_sectors_list from database...")
        sql = self.session.query(SectorModel).statement
        sectors = pd.read_sql(sql=sql, con=self.session.bind)
        return sectors

    def fetch_history(self, **kwargs):
        self.logger.info("Fetching ticker history from database...")
        id = self.session.query(TickerModel).filter_by(**kwargs).first().id
        cls = models.create_ticker_history_model(id)
        sql = self.session.query(cls).statement
        ticker_history = pd.read_sql(sql=sql, con=self.session.bind,
                                           parse_dates=["<DTYYYYMMDD>"])
        return ticker_history

    @property
    def tickers(self):
        return self.__tickers

    @property
    def sectors(self):
        return self.__sectors
