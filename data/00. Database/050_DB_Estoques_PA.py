# Importa bibliotecas
import os
import time
import numpy as np
import pandas as pd
import sqlite3 as sql
from tqdm import tqdm
import scradb
import variables

# Conexão com banco de dados
conn = sql.connect('scra.db')

# Marca tempo de início da execução do programa:
start_time = time.time()

# Estoque de PA
df_est_pa = pd.read_csv(variables.path_est + 'OutputPlanningBook_R05_PA_v20210811_TRANSITO.csv', sep = ';', decimal = ',', dtype = str)
df_est_pa.head(2)

df_est_pa = scradb.est_etl(df_est_pa)
df_est_pa.head(2)

# Agrupadores
df_escopo = pd.read_sql('SELECT * FROM Produtos_Escopo', conn)
df_escopo.head(2)

df_agrup = pd.read_sql('SELECT * FROM Produtos_Agrupadores', conn)
df_agrup = df_agrup[['COD_MATERIAL', 'Agrup']]
df_agrup.head(2)

# Filtrar base considerando apenas PAs do escopo
df_est_pa = df_est_pa[df_est_pa['COD_MATERIAL'].isin(df_agrup['COD_MATERIAL'].unique())]

df_est_pa_escopo = df_est_pa.merge(df_agrup, on = 'COD_MATERIAL')
df_est_pa_escopo = df_est_pa_escopo.rename(columns = {'COD_MATERIAL': 'Material'})
df_est_pa_escopo.head(2)

# Estoque consolidado para PAs
df_est_agg = df_est_pa_escopo.groupby(['DataFoto', 'AnoMes', 'Agrup'], as_index = False).sum()
df_est_agg = df_est_agg.merge(df_escopo[['Agrup', 'Material']], on = 'Agrup')
df_est_agg.head(2)

# Estoque PA mais recente
df_m1_agg = scradb.est_m1_etl(df_est_agg)
# df_m1_escopo = scradb.est_m1_etl(df_est_pa_escopo)

suffix = '_PA'
escopo = '_Escopo'

scradb.table_to_sqlite(df_est_agg, 'Estoque' + suffix)
df_est_agg.to_csv(variables.path_db + variables.csv + 'Estoque'+ suffix + '.csv', index = False)

scradb.table_to_sqlite(df_m1_agg, 'Estoque_M1' + suffix)
df_m1_agg.to_csv(variables.path_db + variables.csv + 'Estoque_M1'+ suffix + '.csv', index = False)

# scradb.table_to_sqlite(df_est_pa_escopo, 'Estoque' + suffix + escopo)
# df_est_pa_escopo.to_csv(variables.path_db + variables.csv + 'Estoque'+ suffix + '.csv', index = False)

# scradb.table_to_sqlite(df_m1_escopo, 'Estoque_M1' + suffix + escopo) 
# df_m1_escopo.to_csv(variables.path_db + variables.csv + 'Estoque_M1' + suffix + escopo + '.csv', index = False)

# Tempo de execução do programa:
finish_time = round(time.time() - start_time, 0)
print("--- {} seconds ---".format(finish_time))
print("--- {} minutes ---".format(round(finish_time / 60, 0)))