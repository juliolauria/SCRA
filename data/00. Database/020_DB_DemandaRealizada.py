# Importa bibliotecas
import os
import time
import numpy as np
import pandas as pd
import copy
import sqlite3 as sql
from datetime import date
import scradb
import variables

conn = sql.connect('scra.db')

# Marca tempo de início da execução do programa:
start_time = time.time()

# Vendas realizadas
# Carrega arquivo de vendas realizadas.
# Base de vendas realizadas disponibilizada pelo time de demanda.
df_dem_real = pd.read_excel(variables.path_dem + 'PREVISTO_REALIZADO_DEMANDA.xlsx', dtype = {'Material': str, 'Mês Ano': str})

# Agrupa base de dados por período (Ano-Mes) e material 
df_dem_real = df_dem_real.rename(columns = {'Material' : 'COD_MATERIAL', 'Mês Ano': 'Mes_Ano'})
df_dem_real = df_dem_real.groupby(['COD_MATERIAL', 'Mes_Ano'], as_index = False).sum()

# Cria coluna "AnoMes"
# Cria colunas de apoio "Ano" e "Mês"
df_dem_real['ANO'] = df_dem_real['Mes_Ano'].apply(lambda x: int(x[:4]))
df_dem_real['MES'] = df_dem_real['Mes_Ano'].apply(lambda x: int(x[-2:]))
df_dem_real = df_dem_real.drop(labels = 'Mes_Ano', axis = 1)
# Coloca zero à esquerda da coluna mês e cria a coluna "AnoMes"
scradb.month_leading_zero(df_dem_real, 'MES')
scradb.anomes(df_dem_real, 'ANO', 'MES', 'AnoMes')

df_dem_real = df_dem_real[['COD_MATERIAL', 'AnoMes', 'Realizado']]

anomes_atual = date.today().strftime('%Y-%m') # Data atual
df_dem_real = df_dem_real[df_dem_real['AnoMes'] != anomes_atual] # Remove realizado do mês corrente, para não considerar parciais
df_dem_real = df_dem_real[df_dem_real['Realizado'] > 0] # Remove registros sem vendas no período

df_dem_real.head()

# Agrupadores
df_dem_real_full, df_dem_real_agrup = scradb.agrup(df_dem_real)
df_dem_real_escopo = scradb.agrup_escopo(df_dem_real_full)

df_dem_real_agrup = df_dem_real_agrup[['AnoMes', 'Agrup', 'Material', 'Realizado']]
df_dem_real_agrup.head()

df_dem_real_escopo = scradb.agrup_escopo(df_dem_real_full)
df_dem_real_escopo.head()

# Exporta dados
suffix = '_Agrup'
full = '_Full'
escopo = '_Escopo'

scradb.table_to_sqlite(df_dem_real_agrup, 'Demanda' + suffix)
df_dem_real_agrup.to_csv(variables.path_db + variables.csv + 'Demanda' + suffix + '.csv', index = False)

scradb.table_to_sqlite(df_dem_real_full, 'Demanda' + suffix + full)
df_dem_real_full.to_csv(variables.path_db + variables.csv + 'Demanda' + suffix + full + '.csv', index = False)

scradb.table_to_sqlite(df_dem_real_escopo, 'Demanda' + suffix + escopo)
df_dem_real_escopo.to_csv(variables.path_db + variables.csv + 'Demanda' + suffix + escopo + '.csv', index = False)

# Tempo de execução do programa:
finish_time = round(time.time() - start_time, 0)
print("--- {} seconds ---".format(finish_time))
print("--- {} minutes ---".format(round(finish_time / 60, 0)))