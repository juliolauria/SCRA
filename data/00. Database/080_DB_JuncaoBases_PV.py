# Importa bibliotecas
import os
import time
import numpy as np
import pandas as pd
import sqlite3 as sql
from tqdm import tqdm
import scradb
import variables

# Conexão com SQLite
conn = sql.connect('scra.db')

# Marca tempo de início da execução do programa:
start_time = time.time()

# Lista de arquivos dos diferentes planos
list_pv = os.listdir(variables.path_dem + variables.plano)
# Tratativa para 202108_PV 
novos_arq = ['202108_PV.csv']
for arq in novos_arq:
    list_pv.remove(arq)

# Carrega os diferentes planos e consolida em um único DataFrame
df_full = pd.DataFrame()
for pv in tqdm(list_pv):
    df_temp = pd.read_csv(variables.path_dem + variables.plano + pv, sep = ';', decimal = ',', dtype = str)
    df_temp['DataFoto'] = pv[:6]
    df_full = pd.concat([df_full, df_temp])

# Tratativa para transformar número do formato brasileiro para americano e converter em float
df_full['Quantidade real'] = df_full['Quantidade real'].str.replace('.', '', regex = False).str.replace(',', '.', regex = False).astype(float)
df_full['Quantidade'] = df_full['Quantidade'].str.replace('.', '', regex = False).str.replace(',', '.', regex = False).astype(float)
df_full.head()

# Arquivo rodada 5
for arq in novos_arq:
    df_temp = pd.read_csv(variables.path_dem + variables.plano + arq, sep = ',', decimal = '.', dtype = str, encoding = 'latin1')
    df_temp['DataFoto'] = arq[:6]
    df_temp['Quantidade real'] = df_temp['Quantidade real'].str.replace(',', '', regex = False).astype(float)
    df_temp['Quantidade'] = df_temp['Quantidade'].str.replace(',', '', regex = False).astype(float)
    df_full = pd.concat([df_full, df_temp])

# Remove demanda internacional ('BA02'):
df_full = df_full[df_full['Unidade gerencial'] != 'BA02']

df_full['DataFoto'] = df_full['DataFoto'].apply(lambda x: x[:4]) + '-' + df_full['DataFoto'].apply(lambda x: x[-2:])
df_full['DataFoto'] = pd.to_datetime(df_full['DataFoto'], format = '%Y-%m')
df_full['AnoMes'] = df_full['Data da demanda'].apply(lambda x: x[-4:]) + '-' + df_full['Data da demanda'].apply(lambda x: x[3:5])
df_full['AnoMes'] = pd.to_datetime(df_full['AnoMes'], format = '%Y-%m')

# Tipos diferentes de demanda
# ['FA', 'FC', 'W0', 'W1', 'ZP']
# Considerar apenas FA  e FC para análise
# Outras são específicas para estoque de contigência e ajustes de sistemas
df_full = df_full[df_full['Tipo de demanda'].isin(['FA', 'FC'])]

# Unidades gerenciais consideradas: DB01, DS01, DE01, e DM01
# df_full['Unidade gerencial'].unique()

df_full = df_full.rename(columns = {'Produto': 'COD_MATERIAL'})
df_full, df = scradb.agrup(df_full, ['DataFoto', 'AnoMes', 'Agrup'])
df_escopo = scradb.agrup_escopo(df_full)
df.head()

df_escopo_eud = pd.read_sql('SELECT * FROM Produtos_Escopo', conn)
df_escopo_eud = df_escopo_eud[df_escopo_eud['UN'] == 'EUD']
df_escopo_eud.head()

df = df[df['Material'].isin(df_escopo_eud['Material'])]

# Cria DataFrame Agrupado
df_agg = df[['Agrup', 'Material', 'Quantidade real', 'Quantidade', 'DataFoto', 'AnoMes']]
df_agg = df_agg.groupby(['Agrup', 'Material', 'DataFoto', 'AnoMes'], as_index = False).sum()

# Calcula diferença em dias e meses entre colunas 'AnoMes' e 'DataFoto'
df_agg['D'] = (df_agg['AnoMes'] - df_agg['DataFoto']) / np.timedelta64(1, 'D')
df_agg['M'] = round((df_agg['AnoMes'] - df_agg['DataFoto']) / np.timedelta64(1, 'M'), 0)

# Ponto de atenção: Existem duas colunas: Quantidade Real e Quantidade para a mesma informação prevista
# Por hora utilizando a coluna 'Quantidade'
df_agg = df_agg.drop(labels = 'Quantidade real', axis = 1)
df_agg = df_agg.rename(columns = {'Quantidade': 'Previsto'})

df_agg['DataFoto'] = df_agg['DataFoto'].astype(str)
df_agg['AnoMes'] = df_agg['AnoMes'].astype(str)

df_agg.head()

# Exporta dados
scradb.table_to_sqlite(df_agg, 'PV')
df_agg.to_csv(variables.csv + 'PV.csv', index = False)

# Tempo de execução do programa:
finish_time = round(time.time() - start_time, 0)
print("--- {} seconds ---".format(finish_time))
print("--- {} minutes ---".format(round(finish_time / 60, 0)))