from .meta import Base
from sqlalchemy import Column, String


class TickerModel(Base):
    __tablename__ = 'tickers'

    id = Column(String, primary_key=True)
    name = Column(String)
    ticker = Column(String)
    latin_name = Column(String)
    latin_ticker = Column(String)
    sector = Column(String)
    market = Column(String)
    sub_market = Column(String)
    ticker_code = Column(String)

    def __repr__(self):
        fmt = '<TickerModel(id="{}", name="{}", ticker="{}", ' \
              'latin_name="{}", latin_ticker="{}", sector="{}",' \
              'market="{}", sub_market="{}", ticker_code="{}")'
        return fmt.format(self.id, self.name, self.ticker, self.latin_name, self.latin_ticker, self.sector, self.market,
                          self.sub_market, self.ticker_code)
