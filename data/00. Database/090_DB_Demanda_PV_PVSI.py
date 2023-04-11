# Importa bibliotecas
import os
import time
import numpy as np
import pandas as pd
import copy
import sqlite3 as sql
from tqdm import tqdm
import scradb
import variables

# Conexão com SQLite
conn = sql.connect('scra.db')

# Marca tempo de início da execução do programa:
start_time = time.time()

# Carrega dados PVSI (Duas bases de dados distintas):
tm1 = pd.read_sql('SELECT * FROM PVSI', conn)
tm1 = tm1.rename(columns = {'SKU': 'COD_MATERIAL'})
tm1.head()

# Agrupadores
df_escopo = pd.read_sql('SELECT * FROM Produtos_Escopo', conn)
df_escopo = df_escopo[['Agrup', 'Material']]

df_agrup = pd.read_sql('SELECT * FROM Produtos_Agrupadores', conn)
df_agrup = df_agrup[['COD_MATERIAL', 'Agrup']]

# Identifica agrupadores do escopo na base de demanda
tm1 = tm1.merge(df_agrup, on = 'COD_MATERIAL', how = 'left')
tm1 = tm1.drop(labels = 'COD_MATERIAL',axis = 1)
tm1 = tm1.groupby(['RODADA', 'RODADA_DATE', 'ANOSEMANA', 'Agrup'], as_index = False).sum()

tm1 = tm1.merge(df_escopo, on = 'Agrup')
material = tm1['Material']
tm1 = tm1.drop(labels = 'Material', axis = 1)
tm1.insert(tm1.columns.get_loc('Agrup') + 1, 'Material', material)

# Renomeia colunas
tm1 = tm1.rename(columns = {'PVSI_LP_BASELINE_SUM': 'PVSI LP BASELINE', 
                            'PVSI_LP_INCREMENTAL_SUM': 'PVSI LP INCREMENTAL', 
                            'PVSI_CP_BASELINE_SUM': 'PVSI CP BASELINE',
                            'PVSI_CP_INCREMENTAL_SUM': 'PVSI CP INCREMENTAL'})

# Cria colunas consolidadas
tm1['PVSI LP'] = tm1['PVSI LP BASELINE'] + tm1['PVSI LP INCREMENTAL']
tm1['PVSI CP'] = tm1['PVSI CP BASELINE'] + tm1['PVSI CP INCREMENTAL']

# Tratativa para identificar período (AnoMes)
tm1['DataSemanaAno'] = pd.to_datetime(tm1['ANOSEMANA'].astype(str) + '0', format='%Y%W%w')
tm1['AnoMes'] = tm1['DataSemanaAno'].astype(str).apply(lambda x: x[:7])
tm1['AnoMes'] = pd.to_datetime(tm1['AnoMes'], format = '%Y-%m')
tm1['DataFoto'] = pd.to_datetime(tm1['RODADA_DATE'])

# Agrupa linhas em função do agrupador, SKU período e data da foto
tm1_agg = tm1.groupby(['RODADA', 'RODADA_DATE', 'DataFoto','Agrup', 'Material', 'AnoMes'], as_index = False).sum().drop(labels = 'ANOSEMANA', axis = 1)
tm1_agg.head()

# Copia da base para tratativas seguintes
tm1_mod = copy.deepcopy(tm1_agg)

# Filtro com colunas pertinentes
tm1_mod = tm1_mod[['RODADA', 'RODADA_DATE', 'DataFoto', 'Agrup', 'Material', 'AnoMes', 'PVSI CP BASELINE', 'PVSI CP INCREMENTAL', 'PVSI CP']]
tm1_mod = tm1_mod.rename(columns = {'PVSI CP BASELINE': 'PV_Baseline', 'PVSI CP INCREMENTAL': 'PV_Incremental', 'PVSI CP': 'Previsto'})

# Cria colunas de proporção da previsão baseline e incremental
tm1_mod['Baseline_Perc'] = tm1_mod['PV_Baseline'] / tm1_mod['Previsto']
tm1_mod['Incremental_Perc'] = tm1_mod['PV_Incremental'] / tm1_mod['Previsto']

# Colunas de diferença entre datas da previsão e foto - Dia e Mês
tm1_mod['D'] = (tm1_mod['AnoMes'] - tm1_mod['DataFoto']) / np.timedelta64(1, 'D')
tm1_mod['M'] = round((tm1_mod['AnoMes'] - tm1_mod['DataFoto']) / np.timedelta64(1, 'M'), 0)

for col in ['DataFoto', 'AnoMes']:
    tm1_mod[col] = tm1_mod[col].astype(str)
    tm1_mod[col] = tm1_mod[col].apply(lambda x: x[:-3])

tm1_mod = tm1_mod.rename(columns = {'SKU': 'Produto', 'RODADA': 'Rodada', 'RODADA_DATE': 'DataRodada'})

tm1_mod.head()

# Lista de produtos que precisam da info de PV
# escopo[~escopo['Material'].isin(tm1_mod['Produto'])]

# Plano de Vendas (Eudora)
df_eud = pd.read_sql('SELECT * FROM PV', conn)

for col in ['DataFoto', 'AnoMes']:
    df_eud[col] = df_eud[col].apply(lambda x: x[:-3])

df_eud.head(2)

# Junção bases TM1 + PV
tm1_mod['Origem'] = 'TM1'
tm1_mod = tm1_mod[['Agrup', 'Material', 'DataFoto', 'AnoMes', 'PV_Baseline', 'PV_Incremental', 'Previsto', 'Baseline_Perc', 'Incremental_Perc', 'D', 'M', 'Origem']]
tm1_mod.head(1)

# Tratativa para deixar base PV (Eudora) no mesmo padrão da PVSI (TM1)
df_eud['PV_Baseline'] = df_eud['Previsto']
df_eud['PV_Incremental'] = 0
df_eud['Baseline_Perc'] = 1
df_eud['Incremental_Perc'] = 0
df_eud['Origem'] = 'PV'
df_eud = df_eud[['Agrup', 'Material', 'DataFoto', 'AnoMes', 'PV_Baseline', 'PV_Incremental', 'Previsto', 'Baseline_Perc', 'Incremental_Perc', 'D', 'M', 'Origem']]
df_eud.head(1)

# Junta bases
tm1_mod = pd.concat([tm1_mod, df_eud])

# Vendas Realizadas
# Carrega informações da tabela Demanda para buscar o valor realizado por mês
df_dem = pd.read_sql('SELECT * FROM Demanda_Agrup', conn)
df_dem.head()

# Filtra tabela e faz tratativas para juntar com tabela de valores previstos
df_dem_real = df_dem[['Agrup', 'Material', 'AnoMes', 'Realizado']]
df_dem_real = df_dem_real.rename(columns = {'COD_MATERIAL': 'Produto'})

df_dem_real.head()

# Junta informações do previsto com o realizado
tm1_mod_real = tm1_mod.merge(df_dem_real, on = ['Agrup', 'Material', 'AnoMes'], how = 'left')
tm1_mod_real = tm1_mod_real[['Agrup', 'Material', 'DataFoto', 'AnoMes', 'D', 'M', 'Origem', 'PV_Baseline', 'PV_Incremental', 'Baseline_Perc', 'Incremental_Perc', 'Previsto', 'Realizado']]

# Cria colunas calculadas
tm1_mod_real['Razao_RealPrev'] = round(tm1_mod_real['Realizado'] / tm1_mod_real['Previsto'], 4) # Razão Realizado / Previsto

tm1_mod_real.head()

# Previsão PV para M4
# Filtro para previsões do M4
tm1_mod_real_m4 = copy.deepcopy(tm1_mod_real)
tm1_mod_real_m4 = tm1_mod_real_m4[tm1_mod_real_m4['M'] >= 4].sort_values('M')
tm1_mod_real_m4 = tm1_mod_real_m4.drop_duplicates(subset = ['Agrup', 'Material', 'AnoMes'])
tm1_mod_real_m4.head()

# Previsão PV para M1
# Filtro para previsão M1 - Mês mais próximo
tm1_mod_real_m1 = copy.deepcopy(tm1_mod_real)
tm1_mod_real_m1 = tm1_mod_real_m1[(tm1_mod_real_m1['M'] >= 1)].sort_values('M')
tm1_mod_real_m1 = tm1_mod_real_m1.drop_duplicates(subset = ['Agrup', 'Material', 'AnoMes'])
tm1_mod_real_m1.head()

# Previsão PV M4M1
m1 = copy.deepcopy(tm1_mod_real_m1)
m4 = copy.deepcopy(tm1_mod_real_m4)

cols = ['PV_Baseline', 'PV_Incremental', 'Baseline_Perc', 'Incremental_Perc', 'Previsto', 'Realizado', 'Razao_RealPrev', 'Razao_RealPrev_Baseline', 'Razao_RealPrev_Incremental']
drop_cols = ['DataFoto', 'D', 'M']

m1 = m1.drop(labels = drop_cols, axis = 1)
m4 = m4.drop(labels = drop_cols, axis = 1)

for col in cols:
    m1 = m1.rename(columns = {col : col + '_M1'})
    m4 = m4.rename(columns = {col : col + '_M4'})

m4m1 = m4.merge(m1, on = ['Agrup', 'Material', 'AnoMes', 'Origem'], how = 'left')
m4m1 = m4m1.dropna()

m4m1['Fator_M4_M1'] = m4m1['Previsto_M1'] / m4m1['Previsto_M4']
m4m1['PV_Baseline_Fator_M4_M1'] = m4m1['PV_Baseline_M1'] / m4m1['PV_Baseline_M4']
m4m1['PV_Incremental_Fator_M4_M1'] = m4m1['PV_Incremental_M1'] / m4m1['PV_Incremental_M4']
m4m1.head()

# Exporta dados
# Salvar arquivos analíticos de demanda full e escopo

scradb.table_to_sqlite(tm1_mod_real, 'Demanda')
tm1_mod_real.to_csv(variables.csv + 'Demanda.csv', index = False)

scradb.table_to_sqlite(tm1_mod_real_m4, 'Demanda_M4')
tm1_mod_real_m4.to_csv(variables.csv + 'Demanda_M4.csv', index = False)

scradb.table_to_sqlite(tm1_mod_real_m1, 'Demanda_M1')
tm1_mod_real_m1.to_csv(variables.csv + 'Demanda_M1.csv', index = False)

scradb.table_to_sqlite(m4m1, 'Demanda_M4M1')
m4m1.to_csv(variables.csv + 'Demanda_M4M1.csv', index = False)

# Tempo de execução do programa:
finish_time = round(time.time() - start_time, 0)
print("--- {} seconds ---".format(finish_time))
print("--- {} minutes ---".format(round(finish_time / 60, 0)))
