========================
binance_historical_data
========================

.. image:: https://img.shields.io/github/last-commit/stas-prokopiev/binance_historical_data
   :target: https://img.shields.io/github/last-commit/stas-prokopiev/binance_historical_data
   :alt: GitHub last commit

.. image:: https://img.shields.io/github/license/stas-prokopiev/binance_historical_data
    :target: https://github.com/stas-prokopiev/binance_historical_data/blob/master/LICENSE.txt
    :alt: GitHub license<space><space>

.. image:: https://img.shields.io/pypi/v/binance_historical_data
   :target: https://img.shields.io/pypi/v/binance_historical_data
   :alt: PyPI

.. image:: https://img.shields.io/pypi/pyversions/binance_historical_data
   :target: https://img.shields.io/pypi/pyversions/binance_historical_data
   :alt: PyPI - Python Version


.. contents:: **Table of Contents**

Short Overview.
=========================
binance_historical_data is a python package (**py>=3.8**)
which makes download of historical crypto data (prices and volumes) from binance server as simple as it can only be.
**You don't even need to have an account at binance.com to download all history of crypto data**

| Data is taken from here: https://data.binance.vision/?prefix=data/spot/
| Dumped locally and then unzipped,
| so you would have an identical local ready to use data copy

| Using this package you will be able to have full historical data of prices and volumes with only 3 lines of python code
| And if you need to update already downloaded data then once again 3 lines of python code will do the job


| **Limitations**: The previous day data appears on binance server a few minutes after 0 a.m. UTC
| So there is a delay in which you can get the data.

Installation via pip:
======================

.. code-block:: bash

    pip install binance_historical_data

How to use it
===========================

Initialize main object: **data_dumper**
---------------------------------------------

.. code-block:: python

    from binance_historical_data import BinanceDataDumper

    data_dumper = BinanceDataDumper(
        path_dir_where_to_dump=".",
        data_type="klines",  # aggTrades, klines, trades
        data_frequency="1m",  # argument for data_type="klines"
    )

Arguments:

#. **path_dir_where_to_dump**:
    | (string) Path to folder where to dump the data
#. **data_type="klines"**:
    | (string) data type to dump: [aggTrades, klines, trades]
#. **str_data_frequency**:
    | (string) One of [1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h]
    | Frequency of price-volume data candles to get

1) The only method to dump the data
------------------------------------------

.. code-block:: python

    data_dumper.dump_data(
        list_tickers=None,
        date_start=None,
        date_end=None,
        is_to_update_existing=False,
    )

Arguments:

#. **list_tickers=None**:
    | (list) Trading pairs for which to dump data
    | *if equals to None* - all **USDT** pairs will be used
#. **date_start=None**:
    | (datetime.date) The date from which to start dump
    | *if equals to None* - every trading pair will be dumped from the early begining (the earliest is 2017-01-01)
#. **date_end=True=None**:
    | (datetime.date) The last date for which to dump data
    | *if equals to None* - Today's date will be used
#. **is_to_update_existing=False**:
    | (bool) Flag if you want to update the data if it's already exist


2) Delete outdated daily results
----------------------------------------------------

Deleta all daily data for which full month monthly data was already dumped

.. code-block:: python

    data_dumper.delete_outdated_daily_results()

.csv klines (candles) files columns
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

| "Open time" - Timestamp
| "Open"
| "High"
| "Low"
| "Close"
| "Volume"
| "Close time" - Timestamp
| "Quote asset volume"
| "Number of trades"
| "Taker buy base asset volume"
| "Taker buy quote asset volume"
| "Ignore"

Examples
===========================

How to dump all data for all USDT trading pairs
------------------------------------------------

Please be advised that the first data dump for all trading pairs might take some time (~40 minutes)

.. code-block:: python

    data_dumper.dump_data()

How to update data (get all new data)
----------------------------------------------

| It's as easy as running the exactly same method **dump_data** once again
| The **data_dumper** will find all the dates for which data already exists
| and will try to dump only the new data

.. code-block:: python

    data_dumper.dump_data()

How to update (reload) data for the asked time period
----------------------------------------------------------

.. code-block:: python

    data_dumper.dump_data(
        date_start=datetime.date(year=2021, month=1, day=1),
        date_end=datetime.date(year=2022, month=1, day=1),
        is_to_update_existing=True
    )

Other useful methods
===========================

Get all trading pairs (tickers) from binance
----------------------------------------------------

.. code-block:: python

    print(data_dumper.get_list_all_trading_pairs())

Get all tickers with locally saved data
----------------------------------------------------

.. code-block:: python

    print(
        data_dumper.get_all_tickers_with_data(timeperiod_per_file="daily")
    )


Get all dates for which there is locally saved data
----------------------------------------------------

.. code-block:: python

    print(
        data_dumper.get_all_dates_with_data_for_ticker(
            ticker,
            timeperiod_per_file="monthly"
        )
    )

Get directory where the local data of exact ticker lies
--------------------------------------------------------

.. code-block:: python

    print(
        data_dumper.get_local_dir_to_data(
            ticker,
            timeperiod_per_file,
        )
    )

Create file name for the local file
----------------------------------------------------

.. code-block:: python

    print(
        data_dumper.create_filename(
            ticker,
            date_obj,
            timeperiod_per_file="monthly",
        )
    )

Links
=====

    * `PYPI <https://pypi.org/project/binance_historical_data/>`_
    * `GitHub <https://github.com/stas-prokopiev/binance_historical_data>`_

Project local Links
===================

    * `CHANGELOG <https://github.com/stas-prokopiev/binance_historical_data/blob/master/CHANGELOG.rst>`_.
    * `CONTRIBUTING <https://github.com/stas-prokopiev/binance_historical_data/blob/master/CONTRIBUTING.rst>`_.

Contacts
========

    * Email: stas.prokopiev@gmail.com
    * `vk.com <https://vk.com/stas.prokopyev>`_
    * `Facebook <https://www.facebook.com/profile.php?id=100009380530321>`_

License
=======

This project is licensed under the MIT License.