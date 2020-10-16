import unittest
from unittest import TestCase
import tfinance as tfin
import pandas as pd

class TestTicker(unittest.TestCase):
    def setUp(self) -> None:
        self.foolad = tfin.Ticker(ticker="فولاد")

    def test_name(self):
        self.assertEqual(self.foolad.name, 'فولاد مباركه اصفهان')

    def test_ticker(self):
        self.assertEqual(self.foolad.ticker, 'فولاد')

    def test_latin_name(self):
        self.assertEqual(self.foolad.latin_name, 'S*Mobarakeh Steel')

    def test_latin_ticker(self):
        self.assertEqual(self.foolad.latin_ticker, 'FOLD1')

    def test_sector(self):
        self.assertEqual(self.foolad.sector, 'فلزات اساسي')

    def test_market(self):
        self.assertEqual(self.foolad.market, 'N1')

    def test_sub_market(self):
        self.assertEqual(self.foolad.sub_market, 'تابلو اصلي')

    def test_ticker_code(self):
        self.assertEqual(self.foolad.ticker_code, 'IRO1FOLD0001')

    def test_history(self):
        self.assertIsInstance(self.foolad.history, pd.DataFrame)


if __name__ == '__main__':
    unittest.main()

