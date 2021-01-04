from datetime import datetime as dt
import logging
import pandas as pd
from edp_crawler import EdpCrawler
from rede import check_if_exists
from setup import Setup


# For testing purpose
# cpe = 'PT0002000038740856ZG'
# date_start = dt(2020, 1, 1)
# date_end = dt.now()
# replace = True
# headless = False
# nr_tries = 1


def run_inst(cpe, date_start=dt(2020, 1, 1), date_end=dt.now(), replace=True, headless=True, nr_tries=1):

    date_range = pd.date_range(date_start, date_end, freq='MS')
    date_range = [d.to_pydatetime() for d in date_range]
    if not replace:
        try_date_range = []
        for d in date_range:
            try:
                is_file = check_if_exists(cpe, d)
                if is_file:
                    print(f"{d} already in rede!")
                else:
                    try_date_range.append(d)
            except Exception as e:
                print(e)
                raise Exception

        if try_date_range:
            try_date_range.sort()
            print(try_date_range)
        else:
            print('No dates missing')
            return
    else:
        try_date_range = date_range

    try:
        bot = EdpCrawler(cpe, try_date_range, replace=replace, headless=headless)
    except Exception as e:
        print(e)
        return

    try:
        bot.login()
        fail_date_range = bot.multiple_tries(nr_tries)
    except:
        fail_date_range = try_date_range

    bot.exit()
    logging.shutdown()

    success_date_range = [d for d in try_date_range if d not in fail_date_range]
    try_date_range = [d.strftime('%m-%Y') for d in try_date_range]
    success_date_range = [d.strftime('%m-%Y') for d in success_date_range]
    fail_date_range = [d.strftime('%m-%Y') for d in fail_date_range]

    logging.info(f'\n\n\nDONE FOR CPE {cpe}\n\n')
    logging.info(
        f'Tried to download {len(try_date_range)} months, succeded in {len(success_date_range)} and failed in {len(fail_date_range)}')

    logging.info(f'TRIED {", ".join(try_date_range)}')
    if success_date_range:
        logging.info(f'SUCCESS IN {", ".join(success_date_range)}')
    if fail_date_range:
        logging.info(f'FAIL IN {", ".join(fail_date_range)}')


def run_mgmt(mgmt, date_start=dt(2020, 1, 1), date_end=dt.now(), replace=True, headless=True, nr_tries=1):
    from db_connection import GetDB
    from edp_crawler import EdpCrawler
    db = GetDB()
    df_inst = db.instalacaoEnergia()
    cpes = df_inst.loc[(df_inst.gestao == mgmt) & (df_inst.abastecimento.isin(['MT', 'BTE']))].cpe.unique()

    for cpe in cpes:
        try:
            run_inst(cpe=cpe, date_start=date_start, date_end=date_end, replace=replace, headless=headless)
        except Exception as e:
            print(e)
            return


# if __name__ == '__main__':
    # run_mgmt('EGEAC')
    # cpe = "PT0002000038740856ZG"
    # run_mgmt('EGEAC', date_start=dt(2020, 10, 1), replace=False, headless=False)

    # cpe = 'PT0002000065174589LK'
    # run_inst(cpe, date_start=dt(2020, 10, 1), replace=True, headless=False)
    # cpe = "PT0002000065171102YF"
    # run_inst(cpe, date_start=dt(2015, 1, 1), replace=False)
