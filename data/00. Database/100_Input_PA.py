import copy
import time
import numpy as np
import pandas as pd
from scipy import stats
import sqlite3 as sql
import scradb

pd.options.display.max_columns = 99
# pd.set_option('display.float_format',lambda x: '{:.4f}'.format(x))

csv = 'CSVs//'

conn = sql.connect('scra.db')

# Marca tempo de início da execução do programa:
start_time = time.time()

# Input PA
# Script feito para construir o input relacionado aos produtos acabados para o modelo 

# Simulação da demanda
# Obtem os parâmentros necessários para a simulação da demanda
# Baseada na distribuição normal truncada e usando as previsões baseline e incremental

# Carrega base de demanda longo prazo
df_dem = pd.read_sql('SELECT * FROM Demanda_M4', conn)
df_dem = df_dem.rename(columns = {'Material': 'Produto'})
df_dem.head(2)

# Remoção de registros do período da pandemia - datas a serem consideradas
datafoto_pandemia = ['2019-10', '2019-11', '2019-12', '2020-01', '2020-02']
anomes_pandemia = ['2020-03', '2020-04', '2020-05', '2020-06', '2020-07', '2020-08']

# Seleciona o mês corrente para remover da análise histórica
current_month = np.sort(df_dem.dropna()['AnoMes'].unique())[-1]
current_month

# Remoção de registros desconsiderados
df_dem_clean = df_dem[df_dem['AnoMes'] != current_month]
df_dem_clean = df_dem_clean[(~df_dem_clean['DataFoto'].isin(datafoto_pandemia)) & (~df_dem_clean['AnoMes'].isin(anomes_pandemia))]
df_dem_clean = df_dem_clean.dropna()
df_dem_clean.head(2)

# Declaração de variáveis
produto_col = 'Produto'
razao_rp = 'Razao_RealPrev'
outliers = 'Outliers'
prod = 'XXXXX'

# Identificação de outliers
df_dem_out = pd.DataFrame()
for pa in df_dem[produto_col].unique():
    df_dem_filt = copy.deepcopy(df_dem_clean)
    df_dem_filt = df_dem_filt[df_dem_filt[produto_col] == pa]

    df_dem_filt[outliers] = 'Não'

    # Definição da quantidade de registroa a serem removidos como outliers em função da base de registros
    lim = (len(df_dem_filt) // 12) + 1

    # Outliers do limite inferior
    df_dem_filt = df_dem_filt.sort_values(razao_rp)
    df_dem_filt.iloc[:lim, df_dem_filt.columns.get_loc(outliers)] = 'Sim'

    # Outliers do limite superior
    df_dem_filt = df_dem_filt.sort_values(razao_rp)
    df_dem_filt.iloc[-lim:, df_dem_filt.columns.get_loc(outliers)] = 'Sim'

    df_dem_out = pd.concat([df_dem_out, df_dem_filt])

# Classifica a base segundo a proporção da previsão incremental presente:
# Baseline: Valor inferiores ou iguais ao fator declarado
# Incremental: Valores superiores ao fator declarado
fator_inc = 0.16
df_dem_out['Abs_Error'] = abs(df_dem_out['Realizado'] - df_dem_out['Previsto'])
df_dem_out['MAPE'] = abs(df_dem_out['Abs_Error'] / df_dem_out['Realizado'])
df_dem_out['Classificacao_Incremental'] = 'Superior a 10%'
df_dem_out.loc[df_dem_out['Incremental_Perc'] <= fator_inc, 'Classificacao_Incremental'] = 'Inferior a 10%'
df_dem_out = df_dem_out.sort_values('AnoMes')
df_dem_out.head(3)

# Função para cálculos por produto de todas as previsões (Desvio padrão)
def mape_dem(df = df_dem_out, group = 'Produto'): 
    df_dem_out_mape = copy.deepcopy(df)
    df_dem_out_mape = df_dem_out_mape[df_dem_out_mape['Outliers'] == 'Não']

    # Cálculo do desvio padrão 
    df_dem_out_std = copy.deepcopy(df_dem_out_mape)
    df_dem_out_std = df_dem_out_std.groupby(group, as_index = False).std()
    df_dem_out_std = df_dem_out_std[[group, 'Razao_RealPrev']]
    df_dem_out_std = df_dem_out_std.rename(columns = {'Razao_RealPrev': 'Razao_RealPrev_Std'})

    # Identificação do valor mínimo histórico
    df_dem_out_min = copy.deepcopy(df_dem_out_mape)
    df_dem_out_min = df_dem_out_min.groupby(group, as_index = False).min()
    df_dem_out_min = df_dem_out_min[[group, 'Razao_RealPrev']]
    df_dem_out_min = df_dem_out_min.rename(columns = {'Razao_RealPrev': 'Prev_Min'})

    # Identificação do valor máximo histórico
    df_dem_out_max = copy.deepcopy(df_dem_out_mape)
    df_dem_out_max = df_dem_out_max.groupby(group, as_index = False).max()
    df_dem_out_max = df_dem_out_max[[group, 'Razao_RealPrev']]
    df_dem_out_max = df_dem_out_max.rename(columns = {'Razao_RealPrev': 'Prev_Max'})

    # Junção das bases
    df_dem_out_std = df_dem_out_std.merge(df_dem_out_min, on = group, how = 'left')
    df_dem_out_std = df_dem_out_std.merge(df_dem_out_max, on = group, how = 'left')

    return df_dem_out_std

# Função para cálculos por produto separando classificação baseline e incremental
def mape_dem_filt(filt, df = df_dem_out, col = 'Classificacao_Incremental', group = 'Produto'): 
    df_dem_out_mape = copy.deepcopy(df)
    df_dem_out_mape = df_dem_out_mape[df_dem_out_mape['Outliers'] == 'Não']
    df_dem_out_mape = df_dem_out_mape[df_dem_out_mape[col] == filt] # Filtro da base por baseline ou incremental

    # Cálculo do desvio padrão
    df_dem_out_std = df_dem_out_mape.groupby(group, as_index = False).std()
    df_dem_out_std = df_dem_out_std[[group, 'Razao_RealPrev']]
    df_dem_out_std = df_dem_out_std.rename(columns = {'Razao_RealPrev': 'Razao_RealPrev_Std'})

    # Número de registros da série baseline/incremental
    df_dem_out_cnt = df_dem_out_mape.groupby(group, as_index = False).count()
    df_dem_out_cnt = df_dem_out_cnt[[group, col]]
    df_dem_out_cnt = df_dem_out_cnt.rename(columns = {col: 'Qtd_Registros'})    
    df_dem_out_cnt['Qtd_Registros'] = df_dem_out_cnt['Qtd_Registros'].astype(int)

    # Identificação do valor mínimo
    df_dem_out_min = df_dem_out_mape.groupby(group, as_index = False).min()
    df_dem_out_min = df_dem_out_min[[group, 'Razao_RealPrev']]
    df_dem_out_min = df_dem_out_min.rename(columns = {'Razao_RealPrev': 'Razao_RealPrev_Min'})

    # Identificação do valor máximo
    df_dem_out_max = df_dem_out_mape.groupby(group, as_index = False).max()
    df_dem_out_max = df_dem_out_max[[group, 'Razao_RealPrev']]
    df_dem_out_max = df_dem_out_max.rename(columns = {'Razao_RealPrev': 'Razao_RealPrev_Max'})

    # Junção das bases para resultado
    df_result = df_dem_out_std.merge(df_dem_out_cnt, on = group, how = 'left')
    df_result = df_result.merge(df_dem_out_min, on = group, how = 'left')
    df_result = df_result.merge(df_dem_out_max, on = group, how = 'left')

    return df_result

# Uso das funções para cálculo das séries
df_mape = mape_dem()

df_mape_base = mape_dem_filt('Inferior a 10%')
df_mape_base = df_mape_base.rename(columns = {'MAPE_Mean': 'MAPE_Mean_Base', 'Razao_RealPrev_Std': 'Razao_RealPrev_Std_Base', 'Qtd_Registros': 'Qtd_Registros_Base',
                                            'Razao_RealPrev_Min': 'Razao_RealPrev_Min_Base', 'Razao_RealPrev_Max': 'Razao_RealPrev_Max_Base'})

df_mape_inc = mape_dem_filt('Superior a 10%')
df_mape_inc = df_mape_inc.rename(columns = {'MAPE_Mean': 'MAPE_Mean_Inc', 'Razao_RealPrev_Std': 'Razao_RealPrev_Std_Inc', 'Qtd_Registros': 'Qtd_Registros_Inc',
                                            'Razao_RealPrev_Min': 'Razao_RealPrev_Min_Inc', 'Razao_RealPrev_Max': 'Razao_RealPrev_Max_Inc'})

# Junção das informações
df_mape = df_mape.merge(df_mape_base, on = 'Produto', how = 'left')
df_mape = df_mape.merge(df_mape_inc, on = 'Produto', how = 'left')
df_mape.head(3)

df_dem_result = df_dem.merge(df_mape, on = 'Produto', how = 'left')
df_dem_out_result = df_dem_out.merge(df_mape, on = 'Produto', how = 'left')

# Exporta dados de demanda para análise
scradb.table_to_sqlite(df_dem_out_result, 'Input_PA_Detail')
df_dem_out_result.to_csv(csv + 'Input_PA_Detail.csv', index = False)

scradb.table_to_sqlite(df_mape, 'Input_PA_Summary')
df_mape.to_csv(csv + 'Input_PA_Summary.csv', index = False)

# Continuação da tratativa para modelo
# Renomeio coluna de material para produto
df_dem_result = df_dem_result.rename(columns = {'Material': 'Produto'})

# Filtra colunas que serão utilizadas no modelo
df_dem_result = df_dem_result[['Produto', 'AnoMes', 'PV_Baseline', 'PV_Incremental', 
                            'Previsto', 'Baseline_Perc', 'Incremental_Perc', 
                            'Prev_Min', 'Prev_Max', 'Razao_RealPrev_Std_Base', 'Razao_RealPrev_Std_Inc']]

# Ponderação por Desvio padrão baseline e Incremental
df_dem_result['Razao_RealPrev_Std_BaseInc'] = (df_dem_result['Razao_RealPrev_Std_Base'] * df_dem_result['Baseline_Perc']) + (df_dem_result['Razao_RealPrev_Std_Inc'] * df_dem_result['Incremental_Perc'])

# Premissa: Desvio padrão do incremental será o dobro do baseline
# df_dem_result['Razao_RealPrev_Std_BaseInc'] = (df_dem_result['Razao_RealPrev_Std_Base'] * df_dem_result['Baseline_Perc']) + (df_dem_result['Razao_RealPrev_Std_Base'] * 2 * df_dem_result['Incremental_Perc'])

# Para casos onde não há info de baseline incremental, repete o valor do baseline (Eudora)
df_dem_result.loc[df_dem_result['Razao_RealPrev_Std_BaseInc'].isna(), 'Razao_RealPrev_Std_BaseInc'] = df_dem_result['Razao_RealPrev_Std_Base']

# Busca informações de Margem Bruta para input
df_precos = pd.read_sql('SELECT * FROM MargemBruta', conn)
df_precos = df_precos.rename(columns = {'ANOMES': 'AnoMes', 'COD_MATERIAL': 'Produto'})

df_dem_result = df_dem_result.merge(df_precos, on = ['AnoMes', 'Produto'], how = 'left')

# Busca valor mais recente de margem bruta dos produtos para serem utilizados em períodos futuros
df_precos_rec = copy.deepcopy(df_precos)
df_precos_rec = df_precos_rec.sort_values('AnoMes', ascending = False)
df_precos_rec = df_precos_rec.drop_duplicates(subset = 'Produto')

# Junta informações em uma coluna única
df_dem_result = df_dem_result.merge(df_precos_rec, on = ['Produto'], how = 'left', suffixes = ['', '_'])

df_dem_result['MB_Unit'] = df_dem_result['MB_Unit'].fillna(0)
df_dem_result['MB_Unit'] = df_dem_result['MB_Unit'] + df_dem_result['MB_Unit_']

df_dem_result = df_dem_result.drop(labels = ['Descrição', 'Descrição_', 'MB_Unit_', 'AnoMes_'], axis = 1)

# Previsão Curto Prazo (M4M1)
# Usado para buscar parâmetros da necessidade de compras extras no curto prazo para flexibilidade 
df_m4m1 = pd.read_sql('SELECT * FROM Demanda_M4M1', conn)
df_m4m1 = df_m4m1.rename(columns = {'Material': 'Produto'})

# Remoção de outliers via zscore
row_m4m1_z = []
for pa in df_m4m1['Produto'].unique():
    df_m4m1_temp = copy.deepcopy(df_m4m1)
    df_m4m1_temp = df_m4m1_temp[df_m4m1_temp['Produto'] == pa]

    df_m4m1_temp.loc[:, 'Previsto_M1_z'] = np.abs(stats.zscore(df_m4m1_temp['Previsto_M1']))
    df_m4m1_temp.loc[:, 'Previsto_M4_z'] = np.abs(stats.zscore(df_m4m1_temp['Previsto_M4']))
    df_m4m1_temp.loc[:, 'Fator_M4_M1_z'] = np.abs(stats.zscore(df_m4m1_temp['Fator_M4_M1']))

    df_m4m1_temp = df_m4m1_temp.loc[df_m4m1_temp['Previsto_M1_z'] < 1, :]
    df_m4m1_temp = df_m4m1_temp.loc[df_m4m1_temp['Previsto_M4_z'] < 1, :]
    df_m4m1_temp = df_m4m1_temp.loc[df_m4m1_temp['Fator_M4_M1_z'] < 1, :]

    # describe = df_m4m1_temp.describe()['Fator_M4_M1']
    # row_m4m1_z.append([pa, describe.loc['min'], describe.loc['50%'], describe.loc['max']])
    row_m4m1_z.append([pa, df_m4m1_temp['Fator_M4_M1'].min(), df_m4m1_temp['Fator_M4_M1'].median(), df_m4m1_temp['Fator_M4_M1'].max()])

df_m = pd.DataFrame(row_m4m1_z, columns = ['Produto', 'M1_Min', 'M1_Median', 'M1_Max'])
df_m

df_dem_result = df_dem_result.merge(df_m, on = ['Produto'])

df_dem_result.head()

# Estoque PA
df_est = pd.read_sql('SELECT * FROM Estoque_M1_PA', conn)
lista_estoque = (list(df_dem_result['Produto'].unique()))
df_est = df_est[df_est['Material'].isin(lista_estoque)]

# O etoque projetado do mês é a quantidade de produtos disponível no final do mês
# O Modelo então irá considerar disponível aquela quantidade para o mês seguinte
# As tratativas seguintes mudam o valor do período para o mês seguinte para fazer a correta consideração
# Ex: Período 2021-11 irá considerar o estoque projetado do fim do mês de 2021-10
# Altera na base 2021-10 para 2021-11
df_est['AnoMes'] = df_est['AnoMes'].astype(np.datetime64())
df_est['AnoMes'] = df_est['AnoMes'] + np.timedelta64(31, 'D')
df_est['AnoMes'] = df_est['AnoMes'].astype(str).apply(lambda x: x[:7])
df_est = df_est.drop(labels = 'M', axis = 1)

df_dem_result = df_dem_result.merge(df_est, left_on = ['Produto', 'AnoMes'], right_on = ['Material', 'AnoMes'], how = 'left').drop(labels = 'Material', axis = 1)
df_dem_result = df_dem_result.rename(columns = {'ESTOQUE_PROJETADO_POSITIVO': 'Estoque_Total_PA', 'ESTOQUE_SEGURANCA_DISP': 'Estoque_Seguranca_PA', 'ESTOQUE_UTIL_POSITIVO': 'Estoque_Util_PA'})
df_dem_result = df_dem_result.drop(labels = ['ESTOQUE_PROJETADO', 'ESTOQUE_SEGURANCA', 'ESTOQUE_UTIL'], axis = 1)

# Tratativa para estoques de PAs sem informação
# Completa info com valor zero
for col in ['Estoque_Total_PA', 'Estoque_Seguranca_PA', 'Estoque_Util_PA']:
    df_dem_result[col] = df_dem_result[col].fillna(0)

# Necessidade de produção = Valor previsto - estoque útil disponível
df_dem_result['Produção'] = df_dem_result['Previsto'] - df_dem_result['Estoque_Util_PA']
df_dem_result.head()

# Filtra Input para conter apenas datas futuras
df_dem_result = df_dem_result[pd.to_datetime(df_dem_result['AnoMes']) > pd.to_datetime(current_month)]
df_dem_result = df_dem_result.sort_values(['Produto', 'AnoMes'])
df_dem_result.head(3)

# Armazena ordem das colunas para tabela final
list_columns = df_dem_result.columns

# Carrrega demanda M1 para usar foto mais recente na previsão dos produtos 
df_m1 = pd.read_sql('SELECT * FROM Demanda_M1', conn)
df_m1 = df_m1[['Material', 'AnoMes', 'PV_Baseline', 'PV_Incremental', 'Previsto', 'Baseline_Perc', 'Incremental_Perc']]
df_m1 = df_m1.rename(columns = {'Material': 'Produto'})
df_m1.head()

# Adequação das colunas de PV
df_dem_result = df_dem_result.drop(['PV_Baseline', 'PV_Incremental', 'Previsto', 'Baseline_Perc', 'Incremental_Perc'], axis = 1)
df_dem_result = df_dem_result.merge(df_m1, on = ['Produto', 'AnoMes'])
df_dem_result = df_dem_result[list_columns]
df_dem_result.head()

# Exporta info de Input PA
scradb.table_to_sqlite(df_dem_result, 'Input_PA')
df_dem_result.to_csv(csv + 'Input_PA.csv', index = False)

# Tempo de execução do programa:
finish_time = round(time.time() - start_time, 0)
print("--- {} seconds ---".format(finish_time))
print("--- {} minutes ---".format(round(finish_time / 60, 0)))