from db_connection import GetDB
from rede import repeat_func, df_to_rede
from clean_data import clean_df
from glob import glob
import os
import pandas as pd
from datetime import datetime as dt
import requests

def main():
    p = r'Z:\DATABASE\ENERGIA\DATAFILES'
    db = GetDB()
    df_inst = db.instalacaoEnergia()

    all_cils_path = repeat_func(glob(os.path.join(p, '*/*')))
    all_cils = [c.split('\\')[-1] for c in all_cils_path]

    for cil_path in all_cils_path:

        # cil_path = all_cils_path[0]
        cil = int(cil_path.split('\\')[-1])

        try:
            inst = df_inst.loc[df_inst.cil == int(cil)].iloc[0]
        except:
            print(f'cant find cil {cil}')

        print(f'\nTrying cpe {inst.cpe}')

        new_dir = os.path.join(r'Z:\DATABASE\ENERGY', inst.cpe)
        files = repeat_func(glob(os.path.join(cil_path, '*')))
        for f in files:
            # f = files[0]
            ym = f.split('\\')[-1].split('_')[1].split('.')[0]
            print(f'-{ym}')

            new_fname = ym + '.xlsx'
            new_fpath = os.path.join(new_dir, new_fname)
            if repeat_func(os.path.isfile(new_fpath)):
                continue
            df = repeat_func(pd.read_excel(f, skiprows=9))
            df = df.iloc[:, :5]
            df.columns = ["date", "hour", "active", "reactiveInductive",  "reactiveCapacitive"]
            df = clean_df(df)
            df_to_rede(cpe=inst.cpe, date=dt(int(ym[:4]), int(ym[4:6]), 1), df=df, replace=False)
        print(f'DONE!')

def find_missing_dates():

    df = pd.read_clipboard()
    df.index = pd.to_datetime(df.Data + ' ' + df.Hora)
    df.sort_index(inplace=True)
    real_date_range = pd.date_range(df.index[0], df.index[-1], freq='15min') 
    missing_dates = [d for d in real_date_range if d not in df.index]
    missing_days = [d.date() for d in missing_dates]
    missing_days = list(set(missing_days))
    missing_months = [d.month for d in missing_days]
    missing_months = list(set(missing_months))
    sep_12 = [d for d in missing_dates if d.month == 9 and d.day == 12]

def req():

    headers = {
    # :authority: online.edpdistribuicao.pt
    # :method: POST
    # :path: /listeners/api.php/ms/reading/data-usage/sgl/get
    # :scheme: https
    # "accept": "application/json, text/plain, */*",
    # "accept-encoding": "gzip, deflate, br",
    # "accept-language": "en-GB,en;q=0.9,en-US;q=0.8,pt;q=0.7,pt-PT;q=0.6",
    # "authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJtc2F1dGgtand0Iiwic3ViIjoiY2Y2NTQ2MjUtYTNmOS00MTRjLWEwZjItMWJhMTM0MTNhMTMwIiwicm9sZXMiOlsiRU5URVJQUklTRSJdLCJpYXQiOjE2MTAwMzMzNDEsImV4cCI6MTYxMDA2MjIwMSwiY2xhaW1zIjpbMF19.BGFtLUlaJ3ibmMtYobqXY1yAOMYXr1qky-JZk9jzVFs",
    # "cache-control": "no-cache",
    # "content-length": "100",
    "content-type": "application/json",
    # "cookie": "_ga=GA1.2.324563167.1599644328; _gid=GA1.2.1893807458.1610033373; PHPSESSID=37c9a285d3399741f746dd8fb88553e2",
    "origin": "https://online.edpdistribuicao.pt",
    "pragma": "no-cache",
    "referer": "https://online.edpdistribuicao.pt/premises/contract/78490bea-8d83-4920-b17b-f777796f4844/consumptions",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "show-loader": "true",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
    }

    import ssl
    ssl._create_default_https_context = ssl._create_unverified_context
    headers = {
    "authority": "online.e-redes.pt",
    "method":"POST",
    "path":"/listeners/api.php/ms/reading/data-usage/sgl/get",
    "scheme":"https",
    "accept ":"application/json, text/plain, */*",
    "accept-encoding":"gzip, deflate, br",
    "accept-language":"en-GB,en;q=0.9,en-US;q=0.8,pt;q=0.7,pt-PT;q=0.6",
    "content-type": "application/json",
    "origin": "https://online.e-redes.pt",
    "pragma": "no-cache",
    "authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJtc2F1dGgtand0Iiwic3ViIjoiYjQ2NGEwYmEtNzJjYy00MDAxLWIwNDQtY2NmYjA4MWYyZGE2Iiwicm9sZXMiOlsiRU5URVJQUklTRSJdLCJpYXQiOjE2MTQxODQ3NTAsImV4cCI6MTYxNDE4ODQxMCwiY2xhaW1zIjpbMF19.8C08DcJbU5AMcKeNw874Y_0P06UvnZ5eu7lRusdO4FM",
    "referer": "https://online.e-redes.pt/listeners/api.php/ms/masterdata/contract/pocket-get-contracts-by-nif",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "show-loader": "true",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36",
    }

    url = "https://online.e-redes.pt/listeners/api.php/ms/masterdata/contract/pocket-get-contracts-by-nif"
    payload = {"CPE": "PT0002000073394972FC", "Year": "2021", "Month": "2", "Power": "1", "Arg10": "EM", "TimeInterval": "1"}

    r = requests.get(url, headers=headers, data=payload)
    r.status_code
    r.content


def filter_cpe(txt):
    if  isinstance(txt, str):
        txt = txt.strip()
        if txt[:4] == 'PT00':
            return txt
    return None

def filter_cpe_2(txt):
    if  isinstance(txt, str):
        txt = txt.split('Local')[0].strip()
        if txt[:4] == 'PT00':
            return txt
    return None

def open_file(fp):
    with open(fp, 'r') as f:
        return f.read()


def get_insts(tensao):
    import pandas as pd
    from glob import glob
    df_total = pd.DataFrame()
    files = glob(f'htmlfiles/{tensao}/*')
    for fp in files:
        # fp = files[0]
        df = pd.read_html(fp)[0]
        df.CPE = df.CPE.map(filter_cpe_2)
        df_total = df_total.append(df)
        df_total = df_total.drop_duplicates('CPE')
    if not df_total.empty:
        df_total.to_excel(f'cpes_cml_{tensao}.xlsx')
    else:
        print(f'no data for {tensao}')

# get_insts('MT')