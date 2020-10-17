__version__ = "0.1.3"
__author__ = "Sadiq Rahmati"

import logging

from .ticker import Ticker
from .market import Market
from .tse_scrapper import TSEScrapper

logging.getLogger("tfinance").addHandler(logging.NullHandler())
