import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from .core import _trim, _ltrim, _rtrim
from .checks import _check_data_types

default_kwargs = {
    "start_date": None,
    "end_date": None,
    "underlying_symbol": 0,
    "underlying_price": 1,
    "option_type": 2,
    "expiration": 3,
    "quote_date": 4,
    "strike": 5,
    "bid": 6,
    "ask": 7,
}


def _add_quote_date(data, file_path):
    file_name = Path(file_path).name
    quote_date = datetime.strptime(os.path.splitext(file_name)[0],
                                   "%Y%m%d-%H%M%S")
    data['quote_date'] = quote_date
    return data


def _filter_option_type(data, option_type_col_num):
    def filter_type(raw_type):
        if 'CE' in raw_type:
            return 'call'
        elif 'PE' in raw_type:
            return 'put'
        else:
            raise ValueError(f'Unknown option type: {raw_type}')

    data['option_type'] = data['option_type'].apply(filter_type)
    return data


def _trim_dates(data, start_date, end_date):
    if start_date is not None and end_date is not None:
        return _trim(data, "expiration", start_date, end_date)
    elif start_date is None and end_date is not None:
        return _rtrim(data, "expiration", end_date)
    elif start_date is not None and end_date is None:
        return _ltrim(data, "expiration", start_date)
    else:
        return data


def _trim_cols(data, column_mapping):
    cols = [col_name for _, col_name in column_mapping if col_name] + \
           ['quote_date']
    return data.loc[:, cols]


def _standardize_cols(data, column_mapping):
    col_names = list(data.columns)
    cols = {col_names[idx]: label for idx, label in column_mapping if idx is not None}
    return data.rename(columns=cols)


def _infer_date_cols(data):
    data["expiration"] = pd.to_datetime(data.expiration, infer_datetime_format=True)
    data["quote_date"] = pd.to_datetime(data.quote_date, infer_datetime_format=True)
    return data


# noinspection PyIncorrectDocstring
def csv_data(file_path, **kwargs):
    """
    Uses pandas DataFrame.read_csv function to import data from CSV files.
    It will automatically generate standardized headers for this library to use.

    Args:
        file_path: str, path to csv file
        start_date: datetime, start date of data set to consider, date is inclusive
        end_date: datetime, end date of data set to consider, date is inclusive
        underlying_symbol: int, index of column containing underlying symbol of option chain
        underlying_price: int, index of column containing underlying stock price
        quote_date: int, index of column containing quote date of option chain
        expiration: int, index of column containing expiration of option chain
        strike: int, index of column containing strike price of option chain
        option_type: int, index of column containing option type of option chain
        bid: int, index of column containing bid price of option chain
        ask: int, index of column containing ask price of option chain

    Returns:
        DataFrame: A dataframe of option chains with standardized columns

    """
    params = {**default_kwargs, **kwargs}

    column_mapping = [
        (params["underlying_symbol"], "underlying_symbol"),
        (params["underlying_price"], "underlying_price"),
        (params["option_type"], "option_type"),
        (params["expiration"], "expiration"),
        (params["quote_date"], "quote_date"),
        (params["strike"], "strike"),
        (params["bid"], "bid"),
        (params["ask"], "ask"),
    ]

    return (
        pd.read_csv(file_path)
        .pipe(_standardize_cols, column_mapping)
        .pipe(_trim_cols, column_mapping)
        .pipe(_infer_date_cols)
        .pipe(_trim_dates, params["start_date"], params["end_date"])
    )


def csv_data_cus(file_path, **kwargs):
    """
    Uses pandas DataFrame.read_csv function to import data from CSV files.
    It will automatically generate standardized headers for this library to use.

    Args:
        file_path: str, path to csv file
        start_date: datetime, start date of data set to consider, date is inclusive
        end_date: datetime, end date of data set to consider, date is inclusive
        underlying_symbol: int, index of column containing underlying symbol of option chain
        underlying_price: int, index of column containing underlying stock price
        quote_date: int, index of column containing quote date of option chain
        expiration: int, index of column containing expiration of option chain
        strike: int, index of column containing strike price of option chain
        option_type: int, index of column containing option type of option chain
        bid: int, index of column containing bid price of option chain
        ask: int, index of column containing ask price of option chain

    Returns:
        DataFrame: A dataframe of option chains with standardized columns

    """
    params = {**default_kwargs, **kwargs}

    column_mapping = [
        (params["underlying_symbol"], "underlying_symbol"),
        (params["underlying_price"], "underlying_price"),
        (params["option_type"], "option_type"),
        (params["expiration"], "expiration"),
        (params["strike"], "strike"),
        (params["bid"], "bid"),
        (params["ask"], "ask"),
    ]

    return (
        pd.read_csv(file_path)
        .pipe(_add_quote_date, file_path)
        .pipe(_standardize_cols, column_mapping)
        .pipe(_filter_option_type, params['option_type'])
        .pipe(_trim_cols, column_mapping)
        .pipe(_infer_date_cols)
        .pipe(_trim_dates, params["start_date"], params["end_date"])
    )
