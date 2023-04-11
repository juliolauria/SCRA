# Importa bibliotecas
import os
import time
import copy
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

# Estoque de Insumos
df_est_ins = pd.read_csv(variables.path_est + 'OutputPlanningBook_R05_IN_v20210819_DEPARA_v2.csv', sep = ';', decimal = ',', dtype = str)
df_est_ins = scradb.est_etl(df_est_ins)
df_est_ins = df_est_ins.rename(columns = {'COD_MATERIAL': 'Material'})
df_est_ins.head(2)

# De-Para Insumos
depara_comp = pd.read_excel(variables.path_prod + 'DE_PARA_MATERIAL_20210806.xlsx', dtype = str)
depara_comp = depara_comp.rename(columns = {'COD_MATERIAL_DE': 'COD_COMPONENTE'})
depara_comp.head()

# Lista de De-Paras manual
# Caso seja necessário alterar o código de algum produto, incluir no arquivo abaixo
depara_comp_manual = pd.read_excel(variables.path_prod + 'SCRA_VIS_DePara_Componentes.xlsx', dtype = str)
depara_comp_manual = depara_comp_manual.rename(columns = {'COD_MATERIAL_DE': 'COD_COMPONENTE'})
depara_comp_manual.head()

depara_comp = pd.concat([depara_comp, depara_comp_manual])
depara_comp = depara_comp.rename(columns = {'COD_COMPONENTE': 'Material'})

df_est_ins = df_est_ins.merge(depara_comp, on = 'Material', how = 'left')
df_est_ins.loc[~df_est_ins['COD_MATERIAL_PARA'].isna(), 'Material'] = df_est_ins.loc[~df_est_ins['COD_MATERIAL_PARA'].isna(), 'COD_MATERIAL_PARA']
df_est_ins = df_est_ins.drop(labels = 'COD_MATERIAL_PARA', axis = 1)

df_est_ins = df_est_ins.groupby(['Material', 'DataFoto', 'AnoMes'], as_index=False).sum()
df_est_ins.head(2)

# Insumos do Escopo
df_bom = pd.read_sql('SELECT * FROM Produtos_ListaTecnica_Principal_Escopo', conn)
df_bom.head(2)
lista_comp = df_bom['COD_COMPONENTE'].unique()
df_est_ins = df_est_ins[df_est_ins['Material'].isin(lista_comp)]

# Estoque mais recente
df_est_m1_ins = copy.deepcopy(df_est_ins)
df_est_m1_ins = scradb.est_m1_etl(df_est_m1_ins)
df_est_m1_ins.head(2)

# Exporta dados

suffix = '_Insumos'

scradb.table_to_sqlite(df_est_ins, 'Estoque' + suffix)
df_est_ins.to_csv(variables.path_db + variables.csv + 'Estoque'+ suffix + '.csv', index = False)

scradb.table_to_sqlite(df_est_m1_ins, 'Estoque_M1' + suffix)
df_est_m1_ins.to_csv(variables.path_db + variables.csv + 'Estoque_M1' + suffix + '.csv', index = False)

# Tempo de execução do programa:
finish_time = round(time.time() - start_time, 0)
print("--- {} seconds ---".format(finish_time))
print("--- {} minutes ---".format(round(finish_time / 60, 0)))