from .meta import Base
from sqlalchemy import Column, String, Integer, DATETIME, FLOAT


class TickerHistoryMixin():
    # __tablename__ = 'ticker_history'

    TICKER = Column("<TICKER>", String)
    DATETIME = Column("<DTYYYYMMDD>" ,DATETIME, primary_key=True)
    FIRST = Column("<FIRST>", FLOAT)
    HIGH = Column("<HIGH>", FLOAT)
    LOW = Column("<LOW>", FLOAT)
    CLOSE = Column("<CLOSE>", FLOAT)
    VALUE = Column("<VALUE>", Integer)
    VOL = Column("<VOL>", Integer)
    OPENINT = Column("<OPENINT>", Integer)
    PER = Column("<PER>", String)
    OPEN = Column("<OPEN>", FLOAT)
    LAST = Column("<LAST>", FLOAT)

    def __repr__(self):
        fmt = '<TickerModel(TICKER="{}", DATETIME="{}", FIRST="{}", ' \
              'HIGH="{}", LOW="{}", CLOSE="{}",' \
              'VALUE="{}", VOL="{}", OPENINT="{}",'\
              'PER="{}", OPEN="{}", LAST="{}")>'
        return fmt.format(self.TICKER, self.DATETIME, self.FIRST, self.HIGH, self.LOW, self.CLOSE, self.VALUE, self.VOL,
                          self.OPENINT, self.PER, self.OPEN, self.LAST)
