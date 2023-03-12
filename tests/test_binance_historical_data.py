


def test_import():
    from binance_historical_data import BinanceDataDumper

def test_main_class_init():
    from binance_historical_data import BinanceDataDumper

    data_dumper = BinanceDataDumper(
        path_dir_where_to_dump=".",
        data_type="klines",  # aggTrades, klines, trades
        data_frequency="1m",  # argument for data_type="klines"
    )

    ## usdm futures
    data_dumper = BinanceDataDumper(
        asset_class="futures\\um",  # spot, futures, margin
        path_dir_where_to_dump="./BianceFutures",
        data_type="klines",  # aggTrades, klines, trades
        data_frequency="1m",  # argument for data_type="klines"
    )

    data_dumper.dump_data(
        tickers='BTCUSDT',
        date_start=None,
        date_end=None,
        is_to_update_existing=False,
        tickers_to_exclude=["UST"],
    )