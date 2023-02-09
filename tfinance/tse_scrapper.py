import concurrent.futures
import errno
import logging
import os
import re
from collections import namedtuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import inspect
from sqlalchemy.types import Integer, Float, String, DateTime
from tqdm import tqdm

from .abc.base_scrapper import BaseScrapper
from .models import TickerModel


class TSEScrapper(BaseScrapper):

    URL_BAZAR_ADDI = "http://www.tsetmc.com/Loader.aspx?ParTree=111C1417"
    URL_TICKER_CSV_TEMPLATE = "http://tsetmc.com/tsev2/data/Export-txt.aspx?t=i&a=1&b=0&i="
    URL_SECTORS_LIST = "http://www.tsetmc.com/Loader.aspx?ParTree=111C1213"
    TEMP_DIR = "csv"
    TickerHistoryFile = namedtuple("TickerHistory", "ticker_id, file_name, response")

    def __init__(self, session):
        self.logger = logging.getLogger(__name__)
        self.session = session
        self.tickers = None
        self.sectors = None
        # Creating ticker_history_set by checking TEMP_DIR
        self.ticker_history_set = set()
        self.logger.info("Creating ./{} directory...".format(self.TEMP_DIR))
        try:
            os.makedirs(self.TEMP_DIR)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        files_list = os.listdir("./{}".format(self.TEMP_DIR))
        self.ticker_history_set = {re.findall("(\d+)\|.*", f)[0] for f in files_list}

    def update(self):
        self.update_tickers()
        self.update_sectors()
        self.update_history()

    def update_tickers(self):
        self.logger.info("Updating tickers ...")
        self.logger.info("Checking whether stocks list table exist or not")
        if inspect(self.session.bind).has_table("tickers"):
            self.logger.info("Loading tickers_list table as a dataframe")
            self.tickers = self.fetch_tickers()
        else:
            self.logger.info("tickers_list table doesn't exits.")
            self.get_tickers()
            self.save_tickers()
        self.logger.info("Updating tickers finished.")

    def update_sectors(self):
        self.logger.info("Updating sectors ...")
        self.logger.info("Checking whether sectors list table exist or not")
        if inspect(self.session.bind).has_table("sectors_list"):
            self.logger.info("Loading sectors list table as a dataframe")
            pass
        else:
            self.logger.info("Sectors list table doesn't exits.")
            self.get_sectors()
            self.save_sectors()
        self.logger.info("Updating sectors finished.")

    def update_history(self):
        self.logger.info("Updating histories ...")
        self.logger.info("Checking stocks history tables' existence")
        self.get_history()
        self.save_history()
        self.logger.info("Updating histories ended.")

    def get_tickers(self):
        self.logger.info("Getting URL_BAZAR_ADDI...")
        r = requests.get(self.URL_BAZAR_ADDI)
        self.logger.info("Creating bazar_adi empty dataframe...")
        bazar_adi_columns = ["id", "name", "ticker", "latin_name", "latin_ticker", "sector", "market", "sub_market",
                             "ticker_code"]
        df_bazar_adi = pd.DataFrame(columns=bazar_adi_columns)
        self.logger.info("Parsing table data into dataframe...")
        html_doc = r.content
        soup = BeautifulSoup(markup=html_doc, features='lxml')
        table = soup.select("table#tblToGrid")[0]
        ID_PATTERN = re.compile(".+code=(.+)")
        for tr in table.find_all(name="tr")[1:]:
            data_row = {}
            for i, td in enumerate(tr.find_all(name="td"), start=1):
                if i == 1:  # ticker_code
                    data_row[bazar_adi_columns[8]] = td.text
                elif i == 2:  # market
                    data_row[bazar_adi_columns[6]] = td.text
                elif i == 3:  # sector
                    data_row[bazar_adi_columns[5]] = td.text
                elif i == 4:  # sub_market
                    data_row[bazar_adi_columns[7]] = td.text
                elif i == 5:  # latin_ticker
                    data_row[bazar_adi_columns[4]] = td.text
                elif i == 6:  # latin_name
                    data_row[bazar_adi_columns[3]] = td.text
                elif i == 7:  # ticker
                    data_row[bazar_adi_columns[2]] = td.text
                elif i == 8:  # name & id
                    data_row[bazar_adi_columns[1]] = td.text
                    m = td.a['href']
                    ticker_id = ID_PATTERN.search(m).groups()[0]
                    data_row[bazar_adi_columns[0]] = ticker_id
            df_bazar_adi = df_bazar_adi.append(data_row, ignore_index=True)
        self.tickers = df_bazar_adi
        self.__filter_tickers()

    def __filter_tickers(self):
        self.r = self.tickers.loc[self.tickers["latin_name"].str.endswith("-R"), :]
        self.tickers = self.tickers.loc[~self.tickers["latin_name"].str.endswith("-R"), :]
        self.d = self.tickers.loc[self.tickers["latin_name"].str.endswith("-D"), :]
        self.tickers = self.tickers.loc[~self.tickers["latin_name"].str.endswith("-D"), :]
        self.tickers.reset_index()

    def fetch_tickers(self):
        self.logger.info("Getting tickers from database...")
        sql = self.session.query(TickerModel).statement
        tickers = pd.DataFrame(self.session.bind.connect().execute(sql))
        return tickers

    def save_tickers(self):
        self.logger.info("Writing df_stock_list to database...")
        self.tickers.to_sql(name="tickers", con=self.session.bind, if_exists="replace",
                                    index=False, chunksize=500,
                                    dtype={"id": String,
                                           "name": String,
                                           "ticker": String,
                                           "latin_name": String,
                                           "latin_ticker": String,
                                           "sector": String,
                                           "market": String,
                                           "sub_market": String,
                                           "ticker_code": String})

    def get_sectors(self):
        self.logger.info("Getting URL_SECTORS_LIST...")
        r = requests.get(self.URL_SECTORS_LIST)
        self.logger.info("Creating sectors_list empty dataframe...")
        sectors_list_columns = ["code", "name"]
        sectors = pd.DataFrame(columns=sectors_list_columns)
        self.logger.info("Parsing table data into dataframe...")
        html_doc = r.content
        soup = BeautifulSoup(markup=html_doc, features="lxml")
        table = soup.select("table#tblToGrid")[0]
        for tr in table.find_all(name="tr")[1:]:
            data_row = {}
            for i, td in enumerate(tr.find_all(name="td"), start=1):
                if i == 1:
                    data_row[sectors_list_columns[0]] = td.text
                elif i == 2:
                    data_row[sectors_list_columns[1]] = td.text
            sectors = sectors.append(data_row, ignore_index=True)
        self.sectors = sectors

    def save_sectors(self):
        self.logger.info("Writing sectors to database...")
        self.sectors.to_sql(name="sectors", con=self.session.bind, if_exists="replace",
                                    index=False, chunksize=500,
                                    dtype={"code": String,
                                           "name": String})

    def get_history(self):
        # Trying to create TEMP_DIR
        self.logger.info("Creating ./{} directory...".format(self.TEMP_DIR))
        try:
            os.makedirs(self.TEMP_DIR)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        self.logger.info("Fetching csv files...")
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for ticker_id in self.tickers["id"]:
                if not self.does_ticker_history_exist(ticker_id):
                    futures.append(executor.submit(self.get_ticker_history, ticker_id=ticker_id))
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                with open("{}/{}".format(self.TEMP_DIR, result.file_name), "wb") as f:
                    f.write(result.response.content)
            self.logger.info("Downloading csv files finished.")

    def save_history(self):
        files_list = os.listdir("./{}".format(self.TEMP_DIR))
        files_id = [re.findall("(\d+)\|.*", f)[0] for f in files_list]
        files = dict(zip(files_id, files_list))
        self.logger.info("Loading csv files as dataframe to memory...")
        tickers_history = {}
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for k, file_name in files.items():  # k=ticker_id & v=file_name
                if not inspect(self.session.bind).has_table(k):
                    futures.append(executor.submit(self.read_ticker_history, file_name))
            for future in concurrent.futures.as_completed(futures):
                ticker_id, df = future.result()
                tickers_history[ticker_id] = df

        self.logger.info("Writing  stocks data to database...")
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for k, df in tickers_history.items():
                kwargs = dict(name=k, con=self.session.bind, if_exists="replace",
                              index=False, chunksize=500,
                              dtype={"<TICKER>": String,
                                     "<DTYYYYMMDD>": DateTime,
                                     "<FIRST>": Float,
                                     "<HIGH> ": Float,
                                     "<LOW>": Float,
                                     "<CLOSE>": Float,
                                     "<VALUE>": Integer,
                                     "<VOL>": Integer,
                                     "<OPENINT>": Integer,
                                     "<PER>": String,
                                     "<OPEN>": Float,
                                     "<LAST>": Float})
                futures.append(executor.submit(df.to_sql, **kwargs))

    def get_ticker_history(self, ticker_id: int) -> TickerHistoryFile:
        self.logger.info("Getting history for ticker with id={}...".format(ticker_id))
        url = self.URL_TICKER_CSV_TEMPLATE + ticker_id
        response = requests.get(url)
        disposition = response.headers['content-disposition']
        file_name = re.findall("filename=(.+)", disposition)[0]
        file_name = ticker_id + '|' + file_name
        return self.TickerHistoryFile(ticker_id, file_name, response)

    def save_ticker_history(self, ticker_history_file: TickerHistoryFile) -> None:
        file_name = ticker_history_file.file_name
        df = pd.read_csv(self.TEMP_DIR + "/" + file_name)
        df['<DTYYYYMMDD>'] = pd.to_datetime(df['<DTYYYYMMDD>'], format='%Y%m%d')
        df.to_sql(name=ticker_history_file.ticker_id, con=self.session.bind, if_exists="replace",
                  index=False, chunksize=500,
                  dtype={"<TICKER>": String,
                         "<DTYYYYMMDD>": DateTime,
                         "<FIRST>": Float,
                         "<HIGH> ": Float,
                         "<LOW>": Float,
                         "<CLOSE>": Float,
                         "<VALUE>": Integer,
                         "<VOL>": Integer,
                         "<OPENINT>": Integer,
                         "<PER>": String,
                         "<OPEN>": Float,
                         "<LAST>": Float})
        return None

    def read_ticker_history(self, file_name):
        df = pd.read_csv(self.TEMP_DIR + "/" + file_name)
        df['<DTYYYYMMDD>'] = pd.to_datetime(df['<DTYYYYMMDD>'], format='%Y%m%d')
        ticker_id = self.get_id_from_history_file(file_name)
        return ticker_id, df

    def does_ticker_history_exist(self, id):
        return str(id) in self.ticker_history_set

    def get_id_from_history_file(self, file_name) -> str:
        return re.findall("(\d+)\|.*", file_name)[0]
