import logging

from .tse import TSE


class Ticker:

    def __init__(self, ticker):
        self.logger = logging.getLogger(__name__)
        tse = TSE()
        df = tse.tickers
        self._info = df[df.ticker == ticker].iloc[0].to_dict()
        self._history = tse.fetch_ticker_history(ticker=ticker)

    @property
    def name(self):
        return self._info["name"]

    @property
    def ticker(self):
        return self._info["ticker"]

    @property
    def latin_name(self):
        return self._info["latin_name"]

    @property
    def latin_ticker(self):
        return self._info["latin_ticker"]

    @property
    def sector(self):
        return self._info["sector"]

    @property
    def market(self):
        return self._info["market"]

    @property
    def sub_market(self):
        return self._info["sub_market"]

    @property
    def ticker_code(self):
        return self._info["ticker_code"]

    @property
    def history(self):
        return self._history