import logging

from .market import Market


class Ticker:

    def __init__(self, **kwargs):
        self.logger = logging.getLogger(__name__)
        market = Market()
        df = market.fetch_tickers_filter_by(**kwargs)
        self._info = df.iloc[0].to_dict()
        self._history = market.fetch_history(**kwargs)

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