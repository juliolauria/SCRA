import time
import numpy as np
import pandas as pd
from scipy import stats
import sqlite3 as sql
import scradb
import matplotlib.pyplot as plt

pd.options.display.max_columns = 99

csv = 'CSVs//'

conn = sql.connect('scra.db')

# Marca tempo de início da execução do programa:
start_time = time.time()

df_bom = pd.read_sql('SELECT * FROM Produtos_ListaTecnica_Principal_Escopo', conn)
df_bom = df_bom.dropna(subset = ['COD_FORNECEDOR'])
df_bom['COD_FORNECEDOR'] = df_bom['COD_FORNECEDOR'].astype(int)
df_bom['COD_FORNECEDOR'] = df_bom['COD_FORNECEDOR'].astype(str)

df_bom = df_bom.groupby(['COD_PA', 'QT_BASICA_PA', 'COD_COMPONENTE', 'DES_COMPONENTE','COD_TIPO_COMPONENTE', 'COD_UM_COMPONENTE', 'COD_FORNECEDOR'], as_index = False).sum() # Agrupa componentes usados em intermediárias ex: Álcool Vinico # - Verificar necessidade desta etapa após modificações 
df_bom['FATOR_COMP_PA'] = df_bom['QT_COMPONENTE'] / df_bom['QT_BASICA_PA'] # Fator unitário
df_bom_filt = df_bom[['COD_PA', 'QT_BASICA_PA', 'COD_COMPONENTE', 'DES_COMPONENTE','COD_TIPO_COMPONENTE', 'QT_COMPONENTE', 'COD_UM_COMPONENTE', 'FATOR_COMP_PA', 'COD_FORNECEDOR']]
df_bom_filt = df_bom_filt.rename(columns = {'COD_PA': 'Produto'})
df_bom_filt.head(3)

df_otif = pd.read_sql('SELECT * FROM OTIF_Summary', conn)
df_otif['Cód'] = df_otif['Cód'].astype(str)

df_otif_filt = df_otif[['Cód', 'Fornecedor', 'OTIF_Min', 'OTIF_Median', 'OTIF_Max', 'Flexibilidade', 'Flex_Min', 'Flex_Median', 'Flex_Max']]
df_otif_filt = df_otif_filt.rename(columns = {'Cód': 'COD_FORNECEDOR'})
df_bom_filt = df_bom_filt.merge(df_otif_filt, on = 'COD_FORNECEDOR', how = 'left')

df_bom_filt.head(3)

df_dem_sim = pd.read_sql('SELECT * FROM Input_PA', conn)
df_bom_filt = df_bom_filt.merge(df_dem_sim[['Produto', 'AnoMes', 'Previsto', 'Produção', 'M1_Min', 'M1_Median', 'M1_Max']], on = 'Produto')
df_bom_filt = df_bom_filt.rename(columns = {'Previsto': 'Previsto_PA', 'Produção': 'Produção_PA'})
df_bom_filt['Produção_Mat'] = df_bom_filt['Produção_PA'] * df_bom_filt['FATOR_COMP_PA']
df_bom_filt['Previsão_Mat'] = df_bom_filt['Previsto_PA'] * df_bom_filt['FATOR_COMP_PA']

# Tratativa para Fornecedores sem Flexibilidade
df_bom_filt['Flexibilidade'] = df_bom_filt['Flexibilidade'].fillna('Manual')
df_bom_filt['Flex_Min'] = df_bom_filt['Flex_Min'].fillna(0)
df_bom_filt['Flex_Median'] = df_bom_filt['Flex_Median'].fillna(0.5)
df_bom_filt['Flex_Max'] = df_bom_filt['Flex_Max'].fillna(1)
df_bom_filt.head(3)

# Estoque insumos
df_est = pd.read_sql('SELECT * FROM Estoque_M1_Insumos', conn)

lista_estoque = (list(df_bom_filt['Produto'].unique()))
lista_estoque = lista_estoque + (list(df_bom_filt['COD_COMPONENTE'].unique()))

df_est = df_est[df_est['Material'].isin(lista_estoque)]

df_est['AnoMes'] = df_est['AnoMes'].astype(np.datetime64())
df_est['AnoMes'] = df_est['AnoMes'] + np.timedelta64(31, 'D')
df_est['AnoMes'] = df_est['AnoMes'].astype(str).apply(lambda x: x[:7])
df_est = df_est.drop(labels = 'M', axis = 1)

df_bom_filt = df_bom_filt.merge(df_est, left_on = ['COD_COMPONENTE', 'AnoMes'], right_on = ['Material', 'AnoMes'], how = 'left').drop(labels = 'Material', axis = 1)
df_bom_filt = df_bom_filt.rename(columns = {'ESTOQUE_PROJETADO_POSITIVO': 'Estoque_Total_Mat', 'ESTOQUE_SEGURANCA_DISP': 'Estoque_Seguranca_Mat', 'ESTOQUE_UTIL_POSITIVO': 'Estoque_Util_Mat'})
df_bom_filt = df_bom_filt.drop(labels = ['ESTOQUE_PROJETADO', 'ESTOQUE_SEGURANCA', 'ESTOQUE_UTIL'], axis = 1)

# Tratativa para estoque não encontrado
for col in ['Estoque_Total_Mat', 'Estoque_Seguranca_Mat', 'Estoque_Util_Mat']:
        df_bom_filt[col] = df_bom_filt[col].fillna(0)

# Quantidade a ser comprada
df_bom_filt['Compras_Meta_Mat'] = df_bom_filt['Produção_Mat'] - df_bom_filt['Estoque_Util_Mat']
df_bom_filt.loc[df_bom_filt['Compras_Meta_Mat'] < 0, 'Compras_Meta_Mat'] = 0

df_bom_filt.head()

# Acordos logísticos
df_acordos = pd.read_sql('SELECT * FROM Produtos_ListaTecnica_Principal_Escopo', conn)
df_acordos = df_acordos[['COD_PA', 'COD_COMPONENTE', 'COD_FORNECEDOR', 'LEAD_TIME_CADASTRADO', 'LEAD_TIME_ACORDO', 'TIPO_ACORDO', 'VOLUME_ACORDADO', 'Acordo_Logistico']]
df_acordos = df_acordos.drop_duplicates()
df_acordos = df_acordos.rename(columns = {'COD_PA': 'Produto'})
df_bom_filt = df_bom_filt.merge(df_acordos, on = ['Produto', 'COD_COMPONENTE', 'COD_FORNECEDOR'], how = 'left')
df_bom_filt.head(2)

scradb.table_to_sqlite(df_bom_filt, 'Input_Insumos')
df_bom_filt.to_csv(csv + 'Input_Insumos.csv', index = False)

# Tempo de execução do programa:
finish_time = round(time.time() - start_time, 0)
print("--- {} seconds ---".format(finish_time))
print("--- {} minutes ---".format(round(finish_time / 60, 0)))