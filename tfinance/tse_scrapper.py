import concurrent
import errno
import logging
import os
import re
from collections import namedtuple

import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Float, String, DateTime
from tqdm import tqdm


class TSEScrapper:
    """
    A TSE Scrapper

    :Example:

    >>> tse_scrapper = TSEScrapper()
    >>> tse_scrapper.get_tickers_list()
    >>> print(tse_scrapper.df_stocks_list.dtypes)
    id              object
    name            object
    ...
    dtype: object
    """

    URL_BAZAR_ADDI = "http://www.tsetmc.com/Loader.aspx?ParTree=111C1417"
    URL_SYMBOL_CSV_TEMPLATE = "http://tsetmc.com/tsev2/data/Export-txt.aspx?t=i&a=1&b=0&i="
    URL_SECTORS_LIST = "http://www.tsetmc.com/Loader.aspx?ParTree=111C1213"
    TEMP_DIR = "csv"
    TickerHistory = namedtuple("TickerHistory", "symbol_id, fname, response")

    def __init__(self, connection_arguments="sqlite:///tse.db"):
        self.logger = logging.getLogger(__name__)
        self.engine = create_engine(connection_arguments)

    def get_tickers_list(self):
        """

        """
        self.logger.info("Getting URL_BAZAR_ADDI...")
        r = requests.get(self.URL_BAZAR_ADDI)
        self.logger.info("Creating bazar_adi empty dataframe...")
        bazar_adi_columns = ["id", "name", "symbol", "latin_name", "latin_symbol", "sector", "market", "sub_market",
                             "symbol_code"]
        df_bazar_adi = pd.DataFrame(columns=bazar_adi_columns)
        self.logger.info("Parsing table data into dataframe...")
        html_doc = r.content
        soup = BeautifulSoup(markup=html_doc, features='lxml')
        table = soup.select("table#tblToGrid")[0]
        ID_PATTERN = re.compile(".+code=(.+)")
        for tr in table.find_all(name="tr")[1:]:
            data_row = {}
            for i, td in enumerate(tr.find_all(name="td"), start=1):
                if i == 1:  # symbol_code
                    data_row[bazar_adi_columns[8]] = td.text
                elif i == 2:  # market
                    data_row[bazar_adi_columns[6]] = td.text
                elif i == 3:  # sector
                    data_row[bazar_adi_columns[5]] = td.text
                elif i == 4:  # sub_market
                    data_row[bazar_adi_columns[7]] = td.text
                elif i == 5:  # latin_symbol
                    data_row[bazar_adi_columns[4]] = td.text
                elif i == 6:  # latin_name
                    data_row[bazar_adi_columns[3]] = td.text
                elif i == 7:  # symbol
                    data_row[bazar_adi_columns[2]] = td.text
                elif i == 8:  # name & id
                    data_row[bazar_adi_columns[1]] = td.text
                    m = td.a['href']
                    symbol_id = ID_PATTERN.search(m).groups()[0]
                    data_row[bazar_adi_columns[0]] = symbol_id
            df_bazar_adi = df_bazar_adi.append(data_row, ignore_index=True)
        self.df_tickers_list = df_bazar_adi
        self.__filter_tickers_list()

    def __filter_tickers_list(self):
        self.r = self.df_tickers_list.loc[self.df_tickers_list["latin_name"].str.endswith("-R"), :]
        self.df_tickers_list = self.df_tickers_list.loc[~self.df_tickers_list["latin_name"].str.endswith("-R"), :]
        self.d = self.df_tickers_list.loc[self.df_tickers_list["latin_name"].str.endswith("-D"), :]
        self.df_tickers_list = self.df_tickers_list.loc[~self.df_tickers_list["latin_name"].str.endswith("-D"), :]
        self.df_tickers_list.reset_index()

    def save_tickers_list(self):
        """

        """
        self.logger.info("Writing  df_stock_list to database...")
        self.df_tickers_list.to_sql(name="tickers_list", con=self.engine, if_exists="replace",
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

    def get_tickers_history(self):
        """

        """
        self.logger.info("Creating ./{} directory...".format(self.TEMP_DIR))
        try:
            os.makedirs(self.TEMP_DIR)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
        self.logger.info("Fetching csv files...")
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for ticker_id in self.df_tickers_list["id"]:
                futures.append(executor.submit(self.get_history, ticker_id=ticker_id))
            for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures)):
                result = future.result()
                with open("{}/{}".format(self.TEMP_DIR, result.fname), "wb") as f:
                    f.write(result.response.content)
            self.logger.info("Downloading csv files finished.")

    def get_history(self, ticker_id: int) -> TickerHistory:
        """

        :param ticker_id:
        :return:
        """
        self.logger.info("Getting history for ticker with id={}...".format(ticker_id))
        url = self.URL_SYMBOL_CSV_TEMPLATE + ticker_id
        response = requests.get(url)
        disposition = response.headers['content-disposition']
        fname = re.findall("filename=(.+)", disposition)[0]
        fname = ticker_id + '|' + fname
        return self.TickerHistory(ticker_id, fname, response)

    def save_tickers_history(self):
        """

        """
        files_list = os.listdir("./{}".format(self.TEMP_DIR))
        files_id = [re.findall("(\d+)\|.*", f)[0] for f in files_list]
        files = dict(zip(files_id, files_list))
        self.logger.info("Loading csv files as dataframe to memory...")
        self.tickers = {}
        for k, v in files.items():  # k=id & v=file_name
            df = pd.read_csv(self.TEMP_DIR + "/" + v)
            df['<DTYYYYMMDD>'] = pd.to_datetime(df['<DTYYYYMMDD>'], format='%Y%m%d')
            self.stocks_data[k] = df
        self.logger.info("Writing  stocks data to database...")
        for k, df in self.stocks_data.items():
            df.to_sql(name=k, con=self.engine, if_exists="replace",
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

    def get_sectors_list(self):
        """

        """
        self.logger.info("Getting URL_SECTORS_LIST...")
        r = requests.get(self.URL_SECTORS_LIST)
        self.logger.info("Creating sectors_list empty dataframe...")
        sectors_list_columns = ["code", "name"]
        df_sectors_list = pd.DataFrame(columns=sectors_list_columns)
        self.logger.info("Parsing table data into dataframe...")
        html_doc = r.content
        soup = BeautifulSoup(markup=html_doc, features="lxml")
        table = soup.select("table#tblToGrid")[0]
        for tr in table.find_all(name="tr")[1:]:
            data_row = {}
            for i, td in enumerate(tr.find_all(name="td"), start=1):
                if (i == 1):
                    data_row[sectors_list_columns[0]] = td.text
                elif (i == 2):
                    data_row[sectors_list_columns[1]] = td.text
            df_sectors_list = df_sectors_list.append(data_row, ignore_index=True)
        self.df_sectors_list = df_sectors_list

    def save_sectors_list(self):
        """

        """
        self.logger.info("Writing df_sectors_list to database...")
        self.df_sectors_list.to_sql(name="sectors_list", con=self.engine, if_exists="replace",
                                    index=False, chunksize=500,
                                    dtype={"code": String,
                                           "name": String})
