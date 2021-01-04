import logging
import os
import time
import pandas as pd
from datetime import datetime as dt
base_dir = r'Z:\DATABASE\ENERGY'


def repeat_func(f):
    """
      Repeat a function multiple times. Useful to connect with rede, since it fails allot.
    """
    tries = 0
    tries_limit = 10
    while tries <= tries_limit:
        try:
            return f
        except Exception as e:
            logging.error(f'Cant run {f.__name__}. Trying again. Problem:\n{e}')
            time.sleep(2)
            tries += 1
    return f


def test_rede():
    # logging.info('----Trying to connect with Rede')
    try:
        a = repeat_func(os.listdir(base_dir))
    except:
        logging.error(' !!Cant connect to rede Z: !! -- Please be sure you have a stable connection before running again.')

        return False

    # logging.info('-----Connection available')
    return True


def create_dir(dir_path):
    if not repeat_func(os.path.isdir(dir_path)):
        repeat_func(os.mkdir(dir_path))


def dt_idx(df):
    df.index = pd.to_datetime(df['date'] + ' ' + df['hour'], format="%Y/%m/%d %H:%M")
    return df


def get_file_path(cpe, date):
    file_name = f'{date.strftime("%Y%m")}.xlsx'
    cpe_dir = os.path.join(base_dir, cpe)
    create_dir(cpe_dir)
    file_path = os.path.join(base_dir, cpe_dir, file_name)
    return file_path


def df_to_rede(cpe, date, df, replace=False):
    result = test_rede()
    if not result:
        return False
    logging.info(f'----Trying to put {cpe} data for {date.strftime("%m-%Y")} in REDE Z:')
    file_path = get_file_path(cpe, date)

    if repeat_func(os.path.isfile(file_path)):
        df_exists = repeat_func(pd.read_excel(file_path))
        df_exists = dt_idx(df_exists)
        df = dt_idx(df)
        common_dates = list(set(df.index.tolist()).intersection(df_exists.index.tolist()))
        if replace:
            df_exists = df_exists.loc[~df_exists.index.isin(common_dates)]
        else:
            df = df.loc[~df.index.isin(common_dates)]

        df = df.append(df_exists)
        df = df[~df.index.duplicated(keep='first')]
        df.sort_index(inplace=True)

    df.reset_index(drop=True, inplace=True)
    repeat_func(df.to_excel(file_path))
    logging.info(f'-----Success!')
    return True


def check_if_exists(cpe, date):
    result = test_rede()

    if result:
        file_path = get_file_path(cpe, date)
        if repeat_func(os.path.isfile(file_path)):
            return True
        else:
            return False
    raise Exception


def get_cpe_data(cpe, date_start=dt(2015, 1, 1), date_end=dt(2020, 10, 1)):
    date_range = pd.date_range(start=date_start, end=date_end, freq='M')
    df = pd.DataFrame()
    for date in date_range:
        # date = date_range[0]
        file_path = get_file_path(cpe, date)
        if repeat_func(os.path.isfile(file_path)):
            df.append(pd.read_excel(fp))
    return df
