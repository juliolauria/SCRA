# Importa bibliotecas
import os
import time
import numpy as np
import pandas as pd
import copy
import sqlite3 as sql
import scradb
import variables

pd.options.display.max_columns = 99

# Conexão com SQLite
conn = sql.connect('scra.db')

# Marca tempo de início da execução do programa:
start_time = time.time()

# Margem bruta
# Carrega arquivo excel com 3 abas, uma para cada ano:
d_precos = {}
df_precos = pd.DataFrame()
for sheet in ['2019', '2020', '2021']:
    d_precos[sheet] = pd.read_excel(variables.path_preco + 'MC1_2019_2021.xlsx', header = 1, sheet_name = sheet, dtype = {'Mês': str})
    df_precos = pd.concat([df_precos, d_precos[sheet]])

# Tratativa para separar produto e descrição
df_precos['Produto'] = df_precos['Material'].apply(lambda x: x.split(' -')[0])
df_precos['Descrição'] = df_precos['Material'].apply(lambda x: x.split(' -')[1][1:])

# Ajustes em colunas
scradb.anomes(df_precos, 'Ano', 'Mês')
df_precos = df_precos[['AnoMes', 'Produto', 'Descrição', 'MB Unit']]
df_precos = df_precos.rename(columns = {'MB Unit': 'MB_Unit'})
df_precos.head()

# Exporta dados
scradb.table_to_sqlite(df_precos, 'MargemBruta')
df_precos.to_csv(variables.path_db + variables.csv + 'MargemBruta.csv', index = False)

# Tempo de execução do programa:
finish_time = round(time.time() - start_time, 0)
print("--- {} seconds ---".format(finish_time))
print("--- {} minutes ---".format(round(finish_time / 60, 0)))