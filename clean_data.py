import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import timedelta
from datetime import datetime as dt


def datetime_df(df):
    """Creates a df with datetime index. For that, has to remove some values. Also fills missing ones
        O problema é que ainda não lida muito bem
        com a mudança de hours, pois no formato do site eles acrescentam, nos
        dias de mudança de hours, 1 hour, ficando o dia com 25 hours.
        Também acho que tiram uma hour no mes oposto(31 de Março e 27 de Outubro).
    """

    # MUDANÇA DE hour! PARA JÁ APAGADA, TAL COMO O VASCO FAZIA
    df = df.query('hour not in ["24:15", "24:30", "24:45", "25:00"]')

    df['date_hour'] = pd.to_datetime(
        df['date'] + ' ' + df['hour'], format='%Y/%m/%d %H:%M')
    df = df.set_index('date_hour')
    df.sort_index(inplace=True)
    # if date_max is not None and date_max in df.date:
    #     df = df.loc[df.date != date_max, :]

    missing_vals = pd.date_range(start=df.index.min(
    ), end=df.index.max(), freq='15min').difference(df.index)
    # print(missing_vals)
    # valor arbitrário, para interpolar caso os dias em falta sejam só 15
    if not missing_vals.empty:
        if len(missing_vals) <= 15:
            df = df[~df.index.duplicated()]
            df = df.reindex(pd.date_range(
                start=df.index[0], end=df.index[-1], freq='15min'))

            df.iloc[:, 1].interpolate(inplace=True)
            df.iloc[:, 3].interpolate(inplace=True)
            df.iloc[:, 4].interpolate(inplace=True)

            df.loc[df.index.isin(missing_vals), 'date'] = df.index[df.index.isin(
                missing_vals)].strftime('%Y/%m/%d')
            df.loc[df.index.isin(missing_vals), 'hour'] = df.index[df.index.isin(
                missing_vals)].strftime('%H:%M')
    # algumas vêm com duplicados pelos vistos?
    df = df.loc[~df.index.duplicated(keep='first')]
    df.columns = ['date', 'hour', 'active',
                  'reactiveInductive', 'reactiveCapacitive']
    return df


def clean_df(df_total, shift_rows=False):
    """cleans df from edp
    """
    try:
        df_total.drop(columns=['id'], inplace=True)
    except:
        pass
    # change 24h to 00
    if not df_total.loc[df_total['hour'] == '24:00', 'hour'].empty:
        df_total.loc[df_total['hour'] == '24:00', 'hour'] = '23:59'
        df_total['date'] = pd.to_datetime(df_total['date'], format='%Y/%m/%d')

        df_total.loc[df_total['hour'] == '23:59', 'date'] = df_total.loc[
            df_total['hour'] == '23:59', 'date'] + timedelta(
            days=1)
        df_total.loc[df_total['hour'] == '23:59', 'hour'] = '00:00'

        # Volta a por a date em string
        df_total['date'] = df_total['date'].dt.strftime('%Y/%m/%d')

    df_total = datetime_df(df_total)
    df_total.sort_index(inplace=True)

    # Shift rows up, to be easier to handle datetime manipulation
    # The "true" data should not be shifted
    if shift_rows:
        # add new row to handle the shift up
        new_begin_row = df_total.iloc[0]
        new_begin_row.name = new_begin_row.name - relativedelta(minutes=15)
        df_total = df_total.append(new_begin_row)
        df_total.sort_index(inplace=True)

        df_total = df_total.shift(-1)
        df_total = df_total.iloc[:-1]

    df_total['date'] = df_total.index.strftime('%Y/%m/%d')
    df_total['hour'] = df_total.index.strftime('%H:%M')

    return df_total
