__version__ = "0.1.0"
__author__ = "Sadiq Rahmati"
import logging

from .ticker import Ticker
from .tse import TSE
from .tse_scrapper import TSEScrapper

logging.getLogger(__name__).addHandler(logging.NullHandler())

