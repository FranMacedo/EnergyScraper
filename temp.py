from db_connection import GetDB
from rede import repeat_func, df_to_rede
from clean_data import clean_df
from glob import glob
import os
import pandas as pd
from datetime import datetime as dt
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


# headers = {
# # :authority: online.edpdistribuicao.pt
# # :method: POST
# # :path: /listeners/api.php/ms/reading/data-usage/sgl/get
# # :scheme: https
# # "accept": "application/json, text/plain, */*",
# # "accept-encoding": "gzip, deflate, br",
# # "accept-language": "en-GB,en;q=0.9,en-US;q=0.8,pt;q=0.7,pt-PT;q=0.6",
# # "authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJtc2F1dGgtand0Iiwic3ViIjoiY2Y2NTQ2MjUtYTNmOS00MTRjLWEwZjItMWJhMTM0MTNhMTMwIiwicm9sZXMiOlsiRU5URVJQUklTRSJdLCJpYXQiOjE2MTAwMzMzNDEsImV4cCI6MTYxMDA2MjIwMSwiY2xhaW1zIjpbMF19.BGFtLUlaJ3ibmMtYobqXY1yAOMYXr1qky-JZk9jzVFs",
# # "cache-control": "no-cache",
# # "content-length": "100",
# "content-type": "application/json",
# # "cookie": "_ga=GA1.2.324563167.1599644328; _gid=GA1.2.1893807458.1610033373; PHPSESSID=37c9a285d3399741f746dd8fb88553e2",
# "origin": "https://online.edpdistribuicao.pt",
# "pragma": "no-cache",
# "referer": "https://online.edpdistribuicao.pt/premises/contract/78490bea-8d83-4920-b17b-f777796f4844/consumptions",
# "sec-fetch-dest": "empty",
# "sec-fetch-mode": "cors",
# "sec-fetch-site": "same-origin",
# "show-loader": "true",
# "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
# }

# import requests
# url = 'https://online.edpdistribuicao.pt/premises/contract/78490bea-8d83-4920-b17b-f777796f4844/consumptions'
# r = requests.get(url, headers=headers)
# r.status_code
# r.content
