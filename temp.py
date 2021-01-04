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
