"""Module with class to download candle historical data from binance"""
# Standard library imports
import os
import re
# from typing import Optional, Any, Union
import urllib.request
import xml.etree.ElementTree as ET
import json
import logging
from collections import defaultdict
from collections import Counter
import zipfile
import datetime
from dateutil.relativedelta import relativedelta

# Third party imports
from tqdm.auto import tqdm
from char import char
from mpire import WorkerPool

# Local imports

# Global constants
LOGGER = logging.getLogger(__name__)


class BinanceDataDumper:
    _FUTURES_ASSET_CLASSES = ("um", "cm")
    _ASSET_CLASSES = ("spot",)
    _DICT_DATA_TYPES_BY_ASSET = {
        "spot": ("aggTrades", "klines", "trades"),
        "cm": ("aggTrades", "klines", "trades", "indexPriceKlines", "markPriceKlines", "premiumIndexKlines"),
        "um": ("aggTrades", "klines", "trades", "indexPriceKlines", "markPriceKlines", "premiumIndexKlines", "metrics")
    }
    _DATA_FREQUENCY_NEEDED_FOR_TYPE = ("klines", "indexPriceKlines", "markPriceKlines", "premiumIndexKlines")
    _DATA_FREQUENCY_ENUM = ('1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h',
                            '1d', '3d', '1w', '1mo')

    def __init__(
            self,
            path_dir_where_to_dump,
            asset_class="spot",  # spot, um, cm
            data_type="klines",  # aggTrades, klines, trades
            data_frequency="1m",  # argument for data_type="klines"
    ) -> None:
        """Init object to dump all data from binance servers

        Args:
            path_dir_where_to_dump (str): Folder where to dump data
            asset_class (str): Asset class which data to get [spot, futures]
            data_type (str): data type to dump: [aggTrades, klines, trades]
            data_frequency (str): \
                Data frequency. [1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h]
                Defaults to "1m".
        """
        if asset_class not in (self._ASSET_CLASSES + self._FUTURES_ASSET_CLASSES):
            raise ValueError(
                f"Unknown asset class: {asset_class} "
                f"not in {self._ASSET_CLASSES + self._FUTURES_ASSET_CLASSES}")

        if data_type not in self._DICT_DATA_TYPES_BY_ASSET[asset_class]:
            raise ValueError(
                f"Unknown data type: {data_type} "
                f"not in {self._DICT_DATA_TYPES_BY_ASSET[asset_class]}")

        if data_type in self._DATA_FREQUENCY_NEEDED_FOR_TYPE:
            if data_frequency not in self._DATA_FREQUENCY_ENUM:
                raise ValueError(
                    f"Unknown data frequency: {data_frequency} "
                    f"not in {self._DATA_FREQUENCY_ENUM}")
            self._data_frequency = data_frequency
        else:
            self._data_frequency = ""

        self.path_dir_where_to_dump = path_dir_where_to_dump
        self.dict_new_points_saved_by_ticker = defaultdict(dict)
        self._base_url = "https://data.binance.vision/data"
        self._asset_class = asset_class
        self._data_type = data_type

    def get_list_all_trading_pairs(self):
        """Get all trading pairs available at binance now"""

        if self._asset_class == 'um':
            response = urllib.request.urlopen("https://fapi.binance.com/fapi/v1/exchangeInfo").read()
        elif self._asset_class == 'cm':
            response = urllib.request.urlopen("https://dapi.binance.com/dapi/v1/exchangeInfo").read()
        else:
            response = urllib.request.urlopen("https://api.binance.com/api/v3/exchangeInfo").read()
        return list(map(
            lambda symbol: symbol['symbol'],
            json.loads(response)['symbols']
        ))

    def _get_list_all_available_files(self, prefix=""):
        """Get all available files from the binance servers"""
        url = os.path.join(self._base_url, prefix).replace("\\", "/").replace("data/", "?prefix=data/")
        response = urllib.request.urlopen(url)
        html_content = response.read().decode('utf-8')

        # Extract the BUCKET_URL variable
        bucket_url_pattern = re.compile(r"var BUCKET_URL = '(.*?)';")
        match = bucket_url_pattern.search(html_content)

        if match:
            bucket_url = match.group(1) + "?delimiter=/&prefix=data/" + prefix.replace('\\', '/') + "/"

            # Retrieve the content of the BUCKET_URL
            bucket_response = urllib.request.urlopen(bucket_url)
            bucket_content = bucket_response.read().decode('utf-8')

            # Parse the XML content and extract all <Key> elements
            root = ET.fromstring(bucket_content)
            # Automatically retrieve the namespace
            namespace = {'s3': root.tag.split('}')[0].strip('{')}
            keys = [element.text for element in root.findall('.//s3:Key', namespace)]
            return keys
        else:
            raise ValueError("BUCKET_URL not found")

    def get_min_start_date_for_ticker(self, ticker):
        """Get minimum start date for ticker"""
        path_folder_prefix = self._get_path_suffix_to_dir_with_data("monthly", ticker)
        min_date = datetime.datetime(datetime.datetime.today().year, datetime.datetime.today().month, 1)

        try:
            date_found = False

            files = self._get_list_all_available_files(prefix=path_folder_prefix)
            for file in files:
                date_str = file.split('.')[0].split('-')[-2:]
                date_str = '-'.join(date_str)
                date_obj = datetime.datetime.strptime(date_str, '%Y-%m')
                if date_obj < min_date:
                    date_found = True
                    min_date = date_obj

            if not date_found:
                path_folder_prefix = self._get_path_suffix_to_dir_with_data("daily", ticker)
                files = self._get_list_all_available_files(prefix=path_folder_prefix)
                for file in files:
                    date_str = file.split('.')[0].split('-')[-3:]
                    date_str = '-'.join(date_str)
                    date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d')
                    if date_obj < min_date:
                        min_date = date_obj

        except Exception as e:
            LOGGER.error('Min date not found: ', e)

        return min_date.date()

    def get_local_dir_to_data(self, ticker, timeperiod_per_file):
        """Path to directory where ticker data is saved

        Args:
            ticker (str): trading pair
            timeperiod_per_file (str): timeperiod per 1 file in [daily, monthly]

        Returns:
            str: path to folder where to save data
        """
        path_folder_suffix = self._get_path_suffix_to_dir_with_data(
            timeperiod_per_file, ticker)
        return os.path.join(
            self.path_dir_where_to_dump, path_folder_suffix)

    @char
    def create_filename(
            self,
            ticker,
            date_obj,
            timeperiod_per_file="monthly",
            extension="csv",
    ):
        """Create file name in the format it's named on the binance server"""

        if timeperiod_per_file == "monthly":
            str_date = date_obj.strftime("%Y-%m")
        else:
            str_date = date_obj.strftime("%Y-%m-%d")

        return f"{ticker}-{self._data_frequency if self._data_frequency else self._data_type}-{str_date}.{extension}"

    def get_all_dates_with_data_for_ticker(
            self,
            ticker,
            timeperiod_per_file="monthly"
    ):
        """Get list with all dates for which there is saved data

        Args:
            ticker (str): trading pair
            timeperiod_per_file (str): timeperiod per 1 file in [daily, monthly]

        Returns:
            list[datetime.date]: dates with saved data
        """
        date_start = datetime.date(year=2017, month=1, day=1)
        date_end = datetime.datetime.utcnow().date()
        list_dates = self._create_list_dates_for_timeperiod(
            date_start,
            date_end,
            timeperiod_per_file=timeperiod_per_file,
        )
        list_dates_with_data = []
        path_folder_suffix = self._get_path_suffix_to_dir_with_data(
            timeperiod_per_file, ticker)
        str_dir_where_to_save = os.path.join(
            self.path_dir_where_to_dump, path_folder_suffix)
        for date_obj in list_dates:
            file_name = self.create_filename(
                ticker,
                date_obj,
                timeperiod_per_file=timeperiod_per_file,
                extension="csv",
            )
            path_where_to_save = os.path.join(
                str_dir_where_to_save, file_name)
            if os.path.exists(path_where_to_save):
                list_dates_with_data.append(date_obj)

        return list_dates_with_data

    def get_all_tickers_with_data(self, timeperiod_per_file="daily"):
        """Get all tickers for which data was dumped

        Args:
            timeperiod_per_file (str): timeperiod per 1 file in [daily, monthly]

        Returns:
            list[str]: all tickers with data
        """
        folder_path = os.path.join(self.path_dir_where_to_dump, self._asset_class)
        folder_path = os.path.join(folder_path, timeperiod_per_file)
        folder_path = os.path.join(folder_path, self._data_type)
        tickers = [
            d
            for d in os.listdir(folder_path)
            if os.path.isdir(os.path.join(folder_path, d))
        ]
        return tickers

    def delete_outdated_daily_results(self):
        """
        Deleta daily data for which full month monthly data was already dumped
        """
        LOGGER.info("Delete old daily data for which there is monthly data")
        dict_files_deleted_by_ticker = defaultdict(int)
        tickers = self.get_all_tickers_with_data(
            timeperiod_per_file="daily")
        for ticker in tqdm(tickers, leave=False):
            list_saved_months_dates = self.get_all_dates_with_data_for_ticker(
                ticker,
                timeperiod_per_file="monthly"
            )
            list_saved_days_dates = self.get_all_dates_with_data_for_ticker(
                ticker,
                timeperiod_per_file="daily"
            )
            for date_saved_day in list_saved_days_dates:
                date_saved_day_tmp = date_saved_day.replace(day=1)
                if date_saved_day_tmp not in list_saved_months_dates:
                    continue
                str_folder = self.get_local_dir_to_data(
                    ticker,
                    timeperiod_per_file="daily",
                )
                str_filename = self.create_filename(
                    ticker,
                    date_saved_day,
                    timeperiod_per_file="daily",
                    extension="csv",
                )
                try:
                    os.remove(os.path.join(str_folder, str_filename))
                    dict_files_deleted_by_ticker[ticker] += 1
                except Exception:
                    LOGGER.warning(
                        "Unable to delete file: %s",
                        os.path.join(str_folder, str_filename)
                    )
        LOGGER.info(
            "---> Done. Daily files deleted for %d tickers",
            len(dict_files_deleted_by_ticker)
        )

    @char
    def dump_data(
            self,
            tickers=None,
            date_start=None,
            date_end=None,
            is_to_update_existing=False,
            int_max_tickers_to_get=None,
            tickers_to_exclude=None,
    ):
        """Main method to dump new of update existing historical data

        Args:
            tickers (list[str]):\
                list trading pairs for which to dump data\
                by default all ****USDT pairs will be taken
            tickers_to_exclude (list[str]):\
                list trading pairs which to exclude from dump
            date_start (datetime.date): Date from which to start dump
            date_end (datetime.date): The last date for which to dump data
            is_to_update_existing (bool): \
                Flag if you want to update data if it's already exists
            int_max_tickers_to_get (int): Max number of trading pairs to get
        """
        self.dict_new_points_saved_by_ticker.clear()
        list_trading_pairs = self._get_list_trading_pairs_to_download(
            tickers=tickers, tickers_to_exclude=tickers_to_exclude)
        if int_max_tickers_to_get:
            list_trading_pairs = list_trading_pairs[:int_max_tickers_to_get]
        LOGGER.info(
            "Download full data for %d tickers: ", len(list_trading_pairs))
        LOGGER.info("---> Data Frequency: %s", self._data_frequency)
        # Start date
        if date_start is None:
            date_start = datetime.date(year=2017, month=1, day=1)
        if date_start < datetime.date(year=2017, month=1, day=1):
            date_start = datetime.date(year=2017, month=1, day=1)
        # End date
        if date_end is None:
            date_end = datetime.datetime.utcnow().date()
        if date_end > datetime.datetime.utcnow().date():
            date_end = datetime.datetime.utcnow().date()
        LOGGER.info("---> Start Date: %s", date_start.strftime("%Y%m%d"))
        LOGGER.info("---> End Date: %s", date_end.strftime("%Y%m%d"))
        date_end_first_day_of_month = datetime.date(
            year=date_end.year, month=date_end.month, day=1)
        for ticker in tqdm(list_trading_pairs, leave=True, desc="Tickers"):
            # 1) Download all monthly data
            if self._data_type != "metrics":
                self._download_data_for_1_ticker(
                    ticker,
                    date_start=date_start,
                    date_end=(date_end_first_day_of_month - relativedelta(days=1)),
                    timeperiod_per_file="monthly",
                    is_to_update_existing=is_to_update_existing,
                )
            # 2) Download all daily date
            if self._data_type != "metrics":
                date_start = date_end_first_day_of_month
            self._download_data_for_1_ticker(
                ticker,
                date_start=date_start,
                date_end=(date_end - relativedelta(days=1)),
                timeperiod_per_file="daily",
                is_to_update_existing=is_to_update_existing,
            )
        #####
        # Print statistics
        self._print_dump_statistics()

    @char
    def _download_data_for_1_ticker(
            self,
            ticker,
            date_start,
            date_end=None,
            timeperiod_per_file="monthly",
            is_to_update_existing=False,
    ):
        """Dump data for 1 ticker"""
        min_start_date = self.get_min_start_date_for_ticker(ticker)
        if date_start < min_start_date:
            date_start = min_start_date
            LOGGER.debug(
                "Start date for ticker %s is %s",
                ticker,
                date_start.strftime("%Y%m%d")
            )
        # Create list dates to use
        list_dates = self._create_list_dates_for_timeperiod(
            date_start,
            date_end=date_end,
            timeperiod_per_file=timeperiod_per_file,
        )
        list_dates_with_data = self.get_all_dates_with_data_for_ticker(
            ticker,
            timeperiod_per_file=timeperiod_per_file
        )
        if is_to_update_existing:
            list_dates_cleared = list_dates
        else:
            list_dates_cleared = [
                date_obj
                for date_obj in list_dates
                if date_obj not in list_dates_with_data
            ]
        LOGGER.debug("Dates to get data: %d", len(list_dates_cleared))
        list_args = [
            (ticker, date_obj, timeperiod_per_file)
            for date_obj in list_dates_cleared
        ]
        # 2) Create path to file where to save data
        str_dir_where_to_save = self.get_local_dir_to_data(
            ticker,
            timeperiod_per_file=timeperiod_per_file,
        )
        if not os.path.exists(str_dir_where_to_save):
            try:
                os.makedirs(str_dir_where_to_save)
            except FileExistsError:
                pass
        #####
        threads = min(len(list_args), 1 if "trades" in self._data_type.lower() else 10)
        with WorkerPool(n_jobs=threads, start_method="threading") as pool:
            list_saved_dates = list(tqdm(
                pool.imap_unordered(
                    self._download_data_for_1_ticker_1_date,
                    list_args
                ),
                leave=False,
                total=len(list_args),
                desc=f"{timeperiod_per_file} files to download",
                unit="files"
            ))
        #####
        list_saved_dates = [date for date in list_saved_dates if date]
        LOGGER.debug(
            "---> Downloaded %d files for ticker: %s",
            len(list_saved_dates),
            ticker
        )
        self.dict_new_points_saved_by_ticker[ticker][
            timeperiod_per_file] = len(list_saved_dates)

    @char
    def _download_data_for_1_ticker_1_date(
            self,
            ticker,
            date_obj,
            timeperiod_per_file="monthly",
    ):
        """Dump data for 1 ticker for 1 data"""
        # 1) Create path to file to save
        path_folder_suffix = self._get_path_suffix_to_dir_with_data(
            timeperiod_per_file, ticker)
        file_name = self.create_filename(
            ticker,
            date_obj,
            timeperiod_per_file=timeperiod_per_file,
            extension="zip",
        )
        str_dir_where_to_save = os.path.join(
            self.path_dir_where_to_dump, path_folder_suffix)
        path_zip_raw_file = os.path.join(
            str_dir_where_to_save, file_name)
        # 2) Create URL to file to download
        url_file_to_download = os.path.join(
            self._base_url, path_folder_suffix, file_name)
        # 3) Download file and unzip it
        if not self._download_raw_file(url_file_to_download, path_zip_raw_file):
            return None
        # 4) Extract zip archive
        try:
            with zipfile.ZipFile(path_zip_raw_file, 'r') as zip_ref:
                zip_ref.extractall(os.path.dirname(path_zip_raw_file))
        except Exception as ex:
            LOGGER.warning(
                "Unable to unzip file %s with error: %s", path_zip_raw_file, ex)
            return None
        # 5) Delete the zip archive
        try:
            os.remove(path_zip_raw_file)
        except Exception as ex:
            LOGGER.warning(
                "Unable to delete zip file %s with error: %s",
                path_zip_raw_file, ex)
            return None
        return date_obj

    def _get_path_suffix_to_dir_with_data(self, timeperiod_per_file, ticker):
        """_summary_

        Args:
            timeperiod_per_file (str): Timeperiod per file: [daily, monthly]
            ticker (str): Trading pair

        Returns:
            str: suffix - https://data.binance.vision/data/ + suffix /filename
        """
        folder_path = ""
        if self._asset_class in self._FUTURES_ASSET_CLASSES:
            folder_path = os.path.join(folder_path, "futures")
        folder_path = os.path.join(folder_path, self._asset_class)
        folder_path = os.path.join(folder_path, timeperiod_per_file)
        folder_path = os.path.join(folder_path, self._data_type)
        folder_path = os.path.join(folder_path, ticker)

        if self._data_frequency:
            folder_path = os.path.join(folder_path, self._data_frequency)

        return folder_path

    @staticmethod
    def _download_raw_file(str_url_path_to_file, str_path_where_to_save):
        """Download file from binance server by URL"""

        LOGGER.debug("Download file from: %s", str_url_path_to_file)
        str_url_path_to_file = str_url_path_to_file.replace("\\", "/")
        try:
            if "trades" not in str_url_path_to_file.lower():
                urllib.request.urlretrieve(str_url_path_to_file, str_path_where_to_save)
            else:  # only show progress bar for trades data as the files are usually big
                with tqdm(unit="B", unit_scale=True, miniters=1,
                          desc="downloading: " + str_url_path_to_file.split("/")[-1]) as progress_bar:
                    def progress_hook(count, block_size, total_size):
                        current_size = block_size * count
                        previous_progress = progress_bar.n / total_size * 100
                        current_progress = current_size / total_size * 100

                        if current_progress > previous_progress + 10:
                            progress_bar.total = total_size
                            progress_bar.update(current_size - progress_bar.n)

                    urllib.request.urlretrieve(
                        str_url_path_to_file, str_path_where_to_save, progress_hook)
        except urllib.error.URLError as ex:
            LOGGER.debug(
                "[WARNING] File not found: %s", str_url_path_to_file)
            return 0
        except Exception as ex:
            LOGGER.warning("Unable to download raw file: %s", ex)
            return 0
        return 1

    def _print_dump_statistics(self):
        """Print the latest dump statistics"""
        LOGGER.info(
            "Tried to dump data for %d tickers:",
            len(self.dict_new_points_saved_by_ticker)
        )
        if len(self.dict_new_points_saved_by_ticker) < 50:
            self._print_full_dump_statististics()
        else:
            self._print_short_dump_statististics()

    def _print_full_dump_statististics(self):
        """"""
        for ticker in self.dict_new_points_saved_by_ticker:
            dict_stats = self.dict_new_points_saved_by_ticker[ticker]
            LOGGER.info(
                "---> For %s new data saved for: %d months %d days",
                ticker,
                dict_stats.get("monthly", 0),
                dict_stats.get("daily", 0),
            )

    def _print_short_dump_statististics(self):
        """"""
        # Gather stats
        int_non_empty_dump_res = 0
        int_empty_dump_res = 0
        list_months_saved = []
        list_days_saved = []
        for ticker in self.dict_new_points_saved_by_ticker:
            dict_stats = self.dict_new_points_saved_by_ticker[ticker]
            list_months_saved.append(dict_stats.get("monthly", 0))
            list_days_saved.append(dict_stats.get("daily", 0))
            if dict_stats["monthly"] or dict_stats["daily"]:
                int_non_empty_dump_res += 1
            else:
                int_empty_dump_res += 1
        #####
        # Print Stats
        LOGGER.info("---> General stats:")
        LOGGER.info(
            "------> NEW Data WAS dumped for %d trading pairs",
            int_non_empty_dump_res)
        LOGGER.info(
            "------> NEW Data WASN'T dumped for %d trading pairs",
            int_empty_dump_res)
        #####
        LOGGER.info("---> New months saved:")
        counter_months = Counter(list_months_saved)
        for value, times in counter_months.most_common(5):
            LOGGER.info("------> For %d tickers saved: %s months", times, value)
        if len(counter_months) > 5:
            LOGGER.info("------> ...")
        LOGGER.info("---> New days saved:")
        counter_days = Counter(list_days_saved)
        for value, times in counter_days.most_common(5):
            LOGGER.info("------> For %d tickers saved: %s days", times, value)
        if len(counter_days) > 5:
            LOGGER.info("------> ...")
        LOGGER.info("=" * 79)

    def _get_list_trading_pairs_to_download(
            self,
            tickers=None,
            tickers_to_exclude=None
    ):
        """
        Create list of tickers for which to get data (by default all **USDT)
        """
        all_tickers = self.get_list_all_trading_pairs()
        LOGGER.info("---> Found overall tickers: %d", len(all_tickers))

        if tickers:
            LOGGER.info("---> Filter to asked tickers: %d", len(tickers))
            tickers_to_use = [
                ticker
                for ticker in all_tickers
                if ticker in tickers
            ]
        else:
            LOGGER.info("---> Filter to USDT tickers")
            tickers_to_use = [
                ticker
                for ticker in all_tickers
                if ticker.endswith("USDT")
            ]
        LOGGER.info("------> Tickers left: %d", len(tickers_to_use))
        #####
        if tickers_to_exclude:
            LOGGER.info("---> Exclude the asked tickers: %d", len(tickers_to_exclude))
            tickers_to_use = [
                ticker
                for ticker in tickers_to_use
                if ticker not in tickers_to_exclude
            ]
            LOGGER.info("------> Tickers left: %d", len(tickers_to_use))
        return tickers_to_use

    @staticmethod
    def _create_list_dates_for_timeperiod(
            date_start,
            date_end=None,
            timeperiod_per_file="monthly",
    ):
        """Create list dates with asked frequency for [date_start, date_end]"""
        list_dates = []
        if date_end is None:
            date_end = datetime.datetime.utcnow().date
        #####
        date_to_use = date_start
        while date_to_use <= date_end:
            list_dates.append(date_to_use)
            if timeperiod_per_file == "monthly":
                date_to_use = date_to_use + relativedelta(months=1)
            else:
                date_to_use = date_to_use + relativedelta(days=1)
        return list_dates
