# Importa bibliotecas
import os
import time
import numpy as np
import pandas as pd
import copy
import sqlite3 as sql
from scipy import stats
import scradb
import variables

# Remove 'warnings' irrelevantes
import warnings
warnings.simplefilter(action = 'ignore', category=UserWarning)
warnings.simplefilter(action = 'ignore', category=FutureWarning)

# Marca tempo de início da execução do programa:
start_time = time.time()

# OTIF
# Leitura e consolidação dos arquivos do OTIF resumido.
# Nome do arquivo.
otif_file = 'Histórico Base OTIF_2018-2021.xlsx'

# Nome das colunas de cada aba da planilha, alterando o nome para cada ano.
list_names_18 = ['PADF_NPADF', 'Planejador', 'Classe 1', 'Classe 2', 'Cód', 'Fornecedor', 'Comprador',	'2018-01',	'2018-02',	'2018-03', '2018-04', '2018-05', '2018-06', '2018-07', '2018-08', '2018-09', '2018-10', '2018-11', '2018-12', 'Média']

list_names_19 = ['PADF_NPADF', 'Planejador', 'Classe 1', 'Classe 2', 'Cód', 'Fornecedor', 'Comprador',	'2019-01',	'2019-02',	'2019-03', '2019-04', '2019-05', '2019-06', '2019-07', '2019-08', '2019-09', '2019-10', '2019-11', '2019-12', 'Média']

list_names_20 = ['PADF_NPADF', 'Planejador', 'Classe 1', 'Classe 2', 'Cód', 'Fornecedor', 'Comprador',	'2020-01',	'2020-02',	'2020-03', '2020-04', '2020-05', '2020-06', '2020-07', '2020-08', '2020-09', '2020-10', '2020-11', '2020-12', 'Média', 'E-mail Fornec', 'Fornecedor Simplificado', 'Classe 1.1', 'Classe 2.1', 'Consta na ZMM422 (S/N)', 'Com Entregas (S/N)']

list_names_21 = ['PADF_NPADF', 'Planejador', 'Classe 1', 'Classe 2', 'Cód', 'Fornecedor', 'Comprador',	'2021-01',	'2021-02',	'2021-03', '2021-04', '2021-05', '2021-06', '2021-07', '2021-08', '2021-09', '2021-10', '2021-11', '2021-12', 'Média', 'E-mail Fornec', 'Fornecedor Simplificado', 'Classe 1.1', 'Classe 2.1', 'Consta na ZMM422 (S/N)', 'Com Entregas (S/N)']

# Filtro de colunas para cada ano.
filter_18 = ['Cód', 'Fornecedor', 'Classe 2', '2018-01',	'2018-02',	'2018-03', '2018-04', '2018-05', '2018-06', '2018-07', '2018-08', '2018-09', '2018-10', '2018-11', '2018-12']

filter_19 = ['Cód', 'Fornecedor', 'Classe 2', '2019-01',	'2019-02',	'2019-03', '2019-04', '2019-05', '2019-06', '2019-07', '2019-08', '2019-09', '2019-10', '2019-11', '2019-12']

filter_20 = ['Cód', 'Fornecedor', 'Classe 2', '2020-01',	'2020-02',	'2020-03', '2020-04', '2020-05', '2020-06', '2020-07', '2020-08', '2020-09', '2020-10', '2020-11', '2020-12']

filter_21 = ['Cód', 'Fornecedor', 'Classe 2', '2021-01',	'2021-02',	'2021-03', '2021-04', '2021-05', '2021-06', '2021-07', '2021-08', '2021-09', '2021-10', '2021-11', '2021-12']

# Carrega arquivos de OTIF em DataFrames separados por ano.
df_otif_18 = pd.read_excel(variables.path_otif + otif_file, sheet_name = 'B1.Base Consolidada - 2018', skiprows=7, names = list_names_18)
df_otif_19 = pd.read_excel(variables.path_otif + otif_file, sheet_name = 'B1.Base Consolidada - 2019', skiprows=7, names = list_names_19)
df_otif_20 = pd.read_excel(variables.path_otif + otif_file, sheet_name = 'B1.Base Consolidada - 2020', skiprows=7, names = list_names_20)
df_otif_21 = pd.read_excel(variables.path_otif + otif_file, sheet_name = 'B1.Base Consolidada - 2021', skiprows=7, names = list_names_21)

# Aplica filtro de colunas.
df_otif_18 = df_otif_18[filter_18]
df_otif_19 = df_otif_19[filter_19]
df_otif_20 = df_otif_20[filter_20]
df_otif_21 = df_otif_21[filter_21]

# Colunas relevantes para agrupamento.
label_cols = ['Cód', 'Fornecedor', 'Classe 2']
# Cria base única com colunas relevantes
df_otif = pd.concat([df_otif_18[label_cols], df_otif_19[label_cols], df_otif_20[label_cols], df_otif_21[label_cols]])
# Remove duplicadas
df_otif = df_otif.drop_duplicates()

# Cria base consolidada, buscando as informações de OTIF de cada ano
df_otif = df_otif.merge(df_otif_18, on=label_cols, how = 'left')
df_otif = df_otif.merge(df_otif_19, on=label_cols, how = 'left')
df_otif = df_otif.merge(df_otif_20, on=label_cols, how = 'left')
df_otif = df_otif.merge(df_otif_21, on=label_cols, how = 'left')

# Substitui marcação 'Sem entrega no período' por nulo (NaN)
df_otif = df_otif.replace('S/ entrega no Período', np.nan)

# Operação 'Unpivot': Passa as colunas selecionadas para linhas, criando a coluna 'AnoMes'
df_otif = df_otif.melt(id_vars = label_cols, value_vars = [
'2018-01',	'2018-02',	'2018-03',	'2018-04', '2018-05', '2018-06', '2018-07', '2018-08', '2018-09', '2018-10', '2018-11', '2018-12',
'2019-01',	'2019-02',	'2019-03',	'2019-04', '2019-05', '2019-06', '2019-07', '2019-08', '2019-09', '2019-10', '2019-11', '2019-12',
'2020-01',	'2020-02',	'2020-03',	'2020-04', '2020-05', '2020-06', '2020-07', '2020-08', '2020-09', '2020-10', '2020-11', '2020-12',
'2021-01',	'2021-02',	'2021-03',	'2021-04', '2021-05', '2021-06', '2021-07', '2021-08', '2021-09', '2021-10', '2021-11', '2021-12'
], var_name = 'AnoMes', value_name = 'OTIF')

# Remove todas as linhas com valor nulo
df_otif = df_otif.dropna()

# Ordena base de dados conforme código do fornecedor e período
df_otif = df_otif.sort_values(['Cód', 'AnoMes'])
df_otif['Cód'] = df_otif['Cód'].astype(str)

df_otif.head()

# Lista de fornecedores
df_forn = df_otif[['Cód', 'Fornecedor', 'Classe 2']].drop_duplicates(subset = 'Cód')
df_forn = df_forn.merge(df_otif[['Cód', 'AnoMes', 'OTIF']], on = 'Cód')

# OTIF Summary
# Cria informação de OTIF resumido (Summary). Sendo uma linha por fornecedor com a informação de OTIF mínimo, mediana e máximo

# Cria lista de colunas a serem removidas
cols_drop = ['AnoMes', 'Fornecedor', 'Classe 2']

# Tratativa de remoção de outliers
# Método de Tukey:

# Para cada fornecedor, filtra a base com o histórico de OTIF.
# Para cada série, calcula o 1º e o 3º quartil (25% e 75% percentil, respectivamente)
# Calcula a diferença entre o 3º e 1º quartil, chamada de diferença interquartil.
# Multiplica a diferença interquartil e obtem o valor limite
# Usando o valor limite, exclui valores abaixo do 1º quartil ou acima do 3º quartil

list_tukey = []
for f in df_forn['Cód'].unique():
    df_filter = df_forn[df_forn['Cód'] == f]
    
    q1 = df_filter["OTIF"].quantile(0.25)
    q3 = df_filter["OTIF"].quantile(0.75)
    iqr = q3 - q1
    valor_limite = 1.5 * iqr
    
    # Limite inferior e Limite superior
    valor_limite_inferior = q1 - valor_limite
    valor_limite_superior = q3 + valor_limite

    list_tukey.append([df_filter['Cód'].unique()[0], df_filter['Fornecedor'].unique()[0], df_filter['Classe 2'].unique()[0], valor_limite_inferior, valor_limite_superior])

# Cria base de dados com informações do método de Tukey
df_tukey = pd.DataFrame(list_tukey, columns=['Cód', 'Fornecedor', 'Classe 2', 'Corte_inferior', 'Corte_superior'])
df_tukey['Cód'] = df_tukey['Cód'].astype(str)
df_tukey.head()

# Cria base de dados OTIF com informações de Tukey, a partir da lista de fornecedores
df_otif_tk = df_forn.merge(df_tukey.drop(labels = ['Fornecedor', 'Classe 2'], axis = 1), on = 'Cód', how = 'left')
df_otif_tk = df_otif_tk[df_otif_tk['OTIF'] >= df_otif_tk['Corte_inferior']]
df_otif_tk = df_otif_tk[df_otif_tk['OTIF'] <= df_otif_tk['Corte_superior']]

# Cria dfs de apoio que sumarizam os dados com valores mínimos e máximos para cada informação

# Variável para coluna 'AnoMes'
anomes = 'AnoMes'

# Valor histórico do OTIF mín, mediano e máx.
df_otif_min_hist = df_forn.groupby('Cód', as_index = False).min().drop(labels = cols_drop, axis = 1).rename(columns = {'OTIF': 'OTIF_Min_Hist'})
df_otif_median_hist = df_forn.groupby('Cód', as_index = False).median().rename(columns = {'OTIF': 'OTIF_Median_Hist'})
df_otif_max_hist = df_forn.groupby('Cód', as_index = False).max().drop(labels = cols_drop, axis = 1).rename(columns = {'OTIF': 'OTIF_Max_Hist'})

# Valores limites calculados pelo método de Tukey
df_otif_tk_cortes = df_otif_tk.groupby('Cód', as_index = False).min().drop_duplicates().drop(cols_drop, 1)

# OTIF min, mediano e max considerados para o modelo, após a remoção de outliers via método de Tukey. 
df_otif_min = df_otif_tk.groupby('Cód', as_index = False).min().rename(columns = {'OTIF': 'OTIF_Min'}).drop(labels = ['Corte_inferior', 'Corte_superior'], axis = 1).drop(labels = cols_drop, axis = 1)
df_otif_median = df_otif_tk.groupby('Cód', as_index = False).median().rename(columns = {'OTIF': 'OTIF_Median'}).drop(labels = ['Corte_inferior', 'Corte_superior'], axis = 1)
df_otif_max = df_otif_tk.groupby('Cód', as_index = False).max().rename(columns = {'OTIF': 'OTIF_Max'}).drop(labels = ['Corte_inferior', 'Corte_superior'], axis = 1).drop(labels = cols_drop, axis = 1)

# Cria df final com informações sumarizadas
# df inicial
df_otif_summary = df_forn[['Cód', 'Fornecedor']].drop_duplicates() 
# Info histórica
df_otif_summary = df_otif_summary.merge(df_otif_min_hist, on = 'Cód', how = 'left')
df_otif_summary = df_otif_summary.merge(df_otif_median_hist, on = 'Cód', how = 'left')
df_otif_summary = df_otif_summary.merge(df_otif_max_hist, on = 'Cód', how = 'left')
# Info dos valores limites de Tukey
df_otif_summary = df_otif_summary.merge(df_otif_tk_cortes, on = 'Cód', how = 'left')
# OTIF considerados
df_otif_summary = df_otif_summary.merge(df_otif_min, on = 'Cód', how = 'left')
df_otif_summary = df_otif_summary.merge(df_otif_median, on = 'Cód', how = 'left')
df_otif_summary = df_otif_summary.merge(df_otif_max, on = 'Cód', how = 'left')

df_otif_summary.head()

# Flexibilidade Qualitativa
flex_file = 'Classificação Flexibilidade Fornecedores Risk Analytics.xlsx'

df_flex = pd.read_excel(variables.path_forn + flex_file, sheet_name = 'Lista_Flexibilidade', dtype = {'Código Fornecedor': str})
df_flex_param = pd.read_excel(variables.path_forn + flex_file, sheet_name = 'Parâmetros')
df_flex_param.head()

df_flex = df_flex.iloc[:, 0:4]
df_flex = df_flex.merge(df_flex_param, on = 'Flexibilidade', how = 'left')
df_flex = df_flex.dropna()
df_flex = df_flex.rename(columns = {'Código Fornecedor': 'Cód'})
df_flex = df_flex[['Cód', 'Flexibilidade', 'Flex_Min', 'Flex_Median', 'Flex_Max']]
df_flex.head()

df_otif_summary = df_otif_summary.merge(df_flex, on = 'Cód', how = 'left')
df_otif_summary.head()

# Exporta dados
scradb.table_to_sqlite(df_otif, 'OTIF')
df_otif.to_csv(variables.path_db + variables.csv + 'OTIF.csv', index = False)

scradb.table_to_sqlite(df_otif_summary, 'OTIF_Summary')
df_otif_summary.to_csv(variables.path_db + variables.csv + 'OTIF_Summary.csv', index = False)

# Tempo de execução do programa:
finish_time = round(time.time() - start_time, 0)
print("--- {} seconds ---".format(finish_time))
print("--- {} minutes ---".format(round(finish_time / 60, 0)))