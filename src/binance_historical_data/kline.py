"""Module with class to download candle historical data from binance"""
# Standard library imports
import os
import urllib.request
import json
import logging
from collections import defaultdict
import zipfile
import datetime
from dateutil.relativedelta import relativedelta

# Third party imports
from tqdm.auto import tqdm
from local_simple_database import LocalDictDatabase
from char import char
from mpire import WorkerPool

# Local imports

# Global constants
LOGGER = logging.getLogger(__name__)


class CandleDataDumper():

    def __init__(
            self,
            path_dir_where_to_dump,
            str_data_frequency="1m",
    ) -> None:
        """Init dumper object

        Args:
            path_dir_where_to_dump (str): Folder where to dump data
            str_data_frequency (str): \
                Data frequency. [1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h]
                Defaults to "1m".
        """
        self._base_url = "https://data.binance.vision"
        self.path_dir_where_to_dump = path_dir_where_to_dump
        self.dict_new_points_saved_by_ticker = defaultdict(dict)
        self.str_data_frequency = str_data_frequency

    def get_list_all_trading_pairs(self):
        """Get all trading pairs available at binance now"""
        response = urllib.request.urlopen(
            "https://api.binance.com/api/v3/exchangeInfo").read()
        return list(map(
            lambda symbol: symbol['symbol'],
            json.loads(response)['symbols']
        ))

    @char
    def dump_data(
            self,
            list_tickers=None,
            date_start=None,
            date_end=None,
            is_to_update_existing=False,
            int_max_tickers_to_get=None,
    ):
        """Main method to dump new of update existing historical data

        Args:
            list_tickers (list[str]):\
                list trading pairs for which to dump data\
                by default all ****USDT pairs will be taken
            date_start (datetime.date): Date from which to start dump
            date_end (datetime.date): The last date for which to dump data
            is_to_update_existing (bool): \
                Flag if you want to update data if it's already exists
            int_max_tickers_to_get (int): Max number of trading pairs to get
        """
        self.dict_new_points_saved_by_ticker.clear()
        list_trading_pairs = self._get_list_trading_pairs_to_download(
            list_tickers=list_tickers)
        if int_max_tickers_to_get:
            list_trading_pairs = list_trading_pairs[:int_max_tickers_to_get]
        LOGGER.info(
            "Download full data for %d tickers: ", len(list_trading_pairs))
        LOGGER.info("---> Data Frequency: %s", self.str_data_frequency)
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
        for str_ticker in tqdm(list_trading_pairs, leave=True, desc="Tickers"):
            # 1) Download all monthly data
            self._download_data_for_1_ticker(
                str_ticker,
                date_start=date_start,
                date_end=(date_end_first_day_of_month-relativedelta(days=1)),
                str_timeperiod_per_file="monthly",
                is_to_update_existing=is_to_update_existing,
            )
            #####
            # 2) Download all daily date
            self._download_data_for_1_ticker(
                str_ticker,
                date_start=date_end_first_day_of_month,
                date_end=(date_end-relativedelta(days=1)),
                str_timeperiod_per_file="daily",
                is_to_update_existing=is_to_update_existing,
            )
        #####
        # Print statistics
        self._print_dump_statistics()
        self._delete_old_daily_results()

    def _print_dump_statistics(self):
        """Print the latest dump statistics"""
        LOGGER.info(
            "Tried to dump data for %d tickers",
            len(self.dict_new_points_saved_by_ticker)
        )
        if len(self.dict_new_points_saved_by_ticker) < 50:
            for str_ticker in self.dict_new_points_saved_by_ticker:
                dict_stats = self.dict_new_points_saved_by_ticker[str_ticker]
                LOGGER.info(
                    "---> For %s new data saved for: %d months %d days",
                    str_ticker,
                    dict_stats["monthly"],
                    dict_stats["daily"],
                )
        else:
            int_non_empty_dump_res = 0
            int_empty_dump_res = 0
            for str_ticker in self.dict_new_points_saved_by_ticker:
                dict_stats = self.dict_new_points_saved_by_ticker[str_ticker]
                if dict_stats["monthly"] or dict_stats["daily"]:
                    int_non_empty_dump_res += 1
                else:
                    int_empty_dump_res += 1
            LOGGER.info(
                "---> NEW Data WAS dumped for %d trading pairs",
                int_non_empty_dump_res)
            LOGGER.info(
                "---> NEW Data WASN'T dumped for %d trading pairs",
                int_empty_dump_res)

    def _get_list_trading_pairs_to_download(self, list_tickers=None):
        """
        Create list of tickers for which to get data (by default all **USDT)
        """
        list_all_trading_pairs = self.get_list_all_trading_pairs()
        if list_tickers:
            return [
                ticker
                for ticker in list_all_trading_pairs
                if ticker in list_tickers
            ]
        return [
            ticker
            for ticker in list_all_trading_pairs
            if ticker.endswith("USDT")
        ]

    def _delete_old_daily_results(self):
        """
        Deleta daily data for which full month monthly data was already dumped
        """
        list_tickers_dirs = [
            d
            for d in os.listdir(self.path_dir_where_to_dump)
            if os.path.isdir(os.path.join(self.path_dir_where_to_dump, d))
        ]
        LOGGER.info("Delete old daily data for which there is monthly data")
        dict_files_deleted_by_ticker = defaultdict(int)
        for str_ticker in tqdm(list_tickers_dirs, leave=False):
            LDD = LocalDictDatabase(
                str_path_database_dir=os.path.join(
                    self.path_dir_where_to_dump,
                    str_ticker,
                    self.str_data_frequency),
                default_value=list(),
            )
            list_saved_days = LDD["dict_list_dates_with_saved_data"]["daily"]
            list_saved_months = LDD["dict_list_dates_with_saved_data"]["monthly"]
            list_days_to_remove = []
            for int_saved_day_date in list_saved_days:
                int_saved_month_date = int_saved_day_date // 100 * 100 + 1
                if int_saved_month_date not in list_saved_months:
                    continue
                str_folder = self._get_local_dir_to_save_data(
                    str_ticker,
                    str_timeperiod_per_file="daily",
                )
                date_obj = datetime.datetime.strftime(
                    str(int_saved_day_date), "%Y%m%d").date()
                str_filename = self._create_filename_to_download(
                    str_ticker,
                    date_obj,
                    str_timeperiod_per_file="daily",
                )
                try:
                    os.remove(os.path.join(str_folder, str_filename))
                    dict_files_deleted_by_ticker[str_ticker] += 1
                    list_days_to_remove.append(int_saved_day_date)
                except Exception:
                    LOGGER.warning(
                        "Unable to delete file: %s",
                        os.path.join(str_folder, str_filename)
                    )
            if list_days_to_remove:
                LDD["dict_list_dates_with_saved_data"]["daily"] = sorted(
                    set(list_saved_days) - set(list_days_to_remove))
        LOGGER.info(
            "---> Done. Daily files deleted for %d tickers",
            len(dict_files_deleted_by_ticker)
        )

    @char
    def _download_data_for_1_ticker(
            self,
            str_ticker,
            date_start,
            date_end=None,
            str_timeperiod_per_file="monthly",
            is_to_update_existing=False,
    ):
        """Dump data for 1 ticker"""
        # Create list dates to use
        list_dates = self._create_list_dates_for_timeperiod(
            date_start,
            date_end=date_end,
            str_timeperiod_per_file=str_timeperiod_per_file,
        )
        LDD = LocalDictDatabase(
            str_path_database_dir=os.path.join(
                self.path_dir_where_to_dump, str_ticker),
            default_value=list(),
        )
        list_dates_with_data = \
            LDD["dict_list_dates_with_saved_data"][str_timeperiod_per_file]
        if is_to_update_existing:
            list_dates_cleared = list_dates
        else:
            list_dates_cleared = [
                date_obj for date_obj in list_dates
                if int(date_obj.strftime("%Y%m%d")) not in list_dates_with_data
            ]
        #####
        list_saved_dates = []
        LOGGER.debug("---> Dates to download data: %d", len(list_dates_cleared))
        list_args = [
            (str_ticker, date_obj, str_timeperiod_per_file)
            for date_obj in list_dates_cleared
        ]
        threads = min(len(list_args), 60)
        with WorkerPool(n_jobs=threads, start_method="threading") as pool:
            list_saved_dates = list(tqdm(
                pool.imap_unordered(
                    self._download_data_for_1_ticker_1_date,
                    list_args
                ),
                leave=False,
                total=len(list_args),
                desc=f"{str_timeperiod_per_file} files to download"
            ))
        list_saved_dates = [
            int(date_obj.strftime("%Y%m%d"))
            for date_obj in list_saved_dates
            if date_obj
        ]
        #####
        # Save saved dates
        if list_saved_dates:
            LDD["dict_list_dates_with_saved_data"][str_timeperiod_per_file] +=\
                sorted(set(list_dates_with_data + list_saved_dates))
        LOGGER.debug(
            "---> Downloaded %d files for ticker: %s",
            len(list_saved_dates),
            str_ticker
        )
        self.dict_new_points_saved_by_ticker[str_ticker][
            str_timeperiod_per_file] = len(list_saved_dates)

    @char
    def _download_data_for_1_ticker_1_date(
            self,
            str_ticker,
            date_obj,
            str_timeperiod_per_file="monthly",
    ):
        """Dump data for 1 ticker for 1 data"""
        # 1) Create URL to file
        path_to_data = (
            f"{self._base_url}/data/spot/{str_timeperiod_per_file}"
            f"/klines/{str_ticker}/{self.str_data_frequency}/"
        )
        file_name = self._create_filename_to_download(
            str_ticker,
            date_obj,
            str_timeperiod_per_file=str_timeperiod_per_file,
        )
        str_url_file_to_download = os.path.join(path_to_data, file_name)
        # 2) Create path to file where to save data
        str_dir_where_to_save = self._get_local_dir_to_save_data(
            str_ticker,
            str_timeperiod_per_file=str_timeperiod_per_file,
        )
        if not os.path.exists(str_dir_where_to_save):
            os.makedirs(str_dir_where_to_save)
        # 3) Download file and unzip it
        str_path_where_to_save = os.path.join(str_dir_where_to_save, file_name)
        if not self._download_raw_file(
            str_url_file_to_download,
            str_path_where_to_save
        ):
            return None
        with zipfile.ZipFile(str_path_where_to_save, 'r') as zip_ref:
            zip_ref.extractall(os.path.dirname(str_path_where_to_save))
        os.remove(str_path_where_to_save)
        return date_obj

    @char
    def _create_filename_to_download(
            self,
            str_ticker,
            date_obj,
            str_timeperiod_per_file="monthly",
    ):
        """Create file name in the format it's named on binance server """
        int_year = int(date_obj.year)
        str_month = str(int(date_obj.month))
        if len(str_month) == 1:
            str_month = "0" + str_month
        if str_timeperiod_per_file == "monthly":
            return f"{str_ticker}-{self.str_data_frequency}-{int_year}-{str_month}.zip"
        str_day = str(int(date_obj.day))
        if len(str_day) == 1:
            str_day = "0" + str_day
        return f"{str_ticker}-{self.str_data_frequency}-{int_year}-{str_month}-{str_day}.zip"

    @char
    def _get_local_dir_to_save_data(
            self,
            str_ticker,
            str_timeperiod_per_file="monthly",
    ):
        """Get folder where to save local data for asked ticker"""
        str_dir_raw = self.path_dir_where_to_dump
        str_dir_raw = os.path.join(str_dir_raw, str_ticker)
        str_dir_raw = os.path.join(str_dir_raw, self.str_data_frequency)
        str_dir_raw = os.path.join(str_dir_raw, str_timeperiod_per_file)
        return str_dir_raw

    @staticmethod
    def _download_raw_file(
            str_url_path_to_file,
            str_path_where_to_save,
    ):
        """Download file from binance server by URL"""
        LOGGER.debug("Download file from: %s", str_url_path_to_file)
        try:
            urllib.request.urlretrieve(
                str_url_path_to_file, str_path_where_to_save)
        except urllib.error.URLError as e:
            # LOGGER.warning("File not found: %s", str_url_path_to_file)
            return 0
        return 1

    @staticmethod
    def _create_list_dates_for_timeperiod(
            date_start,
            date_end=None,
            str_timeperiod_per_file="monthly",
    ):
        """Create list dates with asked frequency for [date_start, date_end]"""
        list_dates = []
        if date_end is None:
            date_end = datetime.datetime.utcnow().date
        #####
        date_to_use = date_start
        while date_to_use <= date_end:
            list_dates.append(date_to_use)
            if str_timeperiod_per_file == "monthly":
                date_to_use = date_to_use + relativedelta(months=1)
            else:
                date_to_use = date_to_use + relativedelta(days=1)
        return list_dates
