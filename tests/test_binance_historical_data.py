


def test_import():
    from binance_historical_data import BinanceDataDumper

def test_main_class_init():
    from binance_historical_data import BinanceDataDumper

    data_dumper = BinanceDataDumper(
        path_dir_where_to_dump=".",
        data_type="klines",  # aggTrades, klines, trades
        data_frequency="1m",  # argument for data_type="klines"
    )
