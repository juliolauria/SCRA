import os
import time
import numpy as np
import pandas as pd
import sqlite3 as sql
import scradb
import variables

# Conexão com SQLite
conn = sql.connect('scra.db')

# Marca tempo de início da execução do programa:
start_time = time.time()

# PVSI
# Junção de bases
df = pd.DataFrame()
pvsi_list = [c for c in os.listdir(variables.path_dem + variables.pvsi) if '.csv' in c]
for c in pvsi_list:
    df_temp = pd.read_csv(variables.path_dem + variables.pvsi + c)
    df_temp.columns = df_temp.columns.str.upper().str.replace(' ', '_')
    df = pd.concat([df, df_temp])

df['SKU'] = df['SKU'].astype(str)
df['RODADA_DATE'] = df['RODADA_DATE'].apply(lambda x: str(x)[:10])

# Exporta dados
scradb.table_to_sqlite(df, 'PVSI')
df.to_csv(variables.csv + 'PVSI.csv', index = False)

# Tempo de execução do programa:
finish_time = round(time.time() - start_time, 0)
print("--- {} seconds ---".format(finish_time))
print("--- {} minutes ---".format(round(finish_time / 60, 0)))