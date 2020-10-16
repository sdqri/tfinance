import unittest
from unittest import TestCase

import tfinance as tfin
import pandas as pd


class TestMarket(unittest.TestCase):

    def setUp(self) -> None:
        self.market = tfin.Market()

    # Test Market's tickers property
    def test_tickers(self):
        tickers = self.market.tickers
        self.assertIsInstance(tickers, pd.DataFrame)

    # Test Market's sectors property
    def test_sectors(self):
        sectors = self.market.sectors
        self.assertIsInstance(sectors, pd.DataFrame)

    # Test fetch_history_method
    def test_fetch_history(self):
        history = self.market.fetch_history(ticker="فولاد")
        self.assertIsInstance(history, pd.DataFrame)


if __name__ == '__main__':
    unittest.main()
