from .meta import Base, Session
from .ticker_model import TickerModel
from .sector_model import SectorModel
from .ticker_history_mixin import TickerHistoryMixin


def create_ticker_history_model(id):
    name = "TickerHistoryModel_{}".format(id)
    cls = type(name, (TickerHistoryMixin, Base),
               {"__tablename__": id, "__table_args__" : {'extend_existing': True}})
    return cls
