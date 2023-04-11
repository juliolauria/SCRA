# Importa bibliotecas
import os
import time
import copy
import sqlite3 as sql
import numpy as np
import pandas as pd
from tqdm import tqdm
from scipy import stats
import matplotlib.pyplot as plt
import scra_montecarlo

# Diretório do Banco de Dados
path_db = '..//..//Data//00. Banco de Dados//'

# Aumenta número máximo de colunas para 99
pd.options.display.max_columns = 99

# Conexão com SQLite
conn = sql.connect(path_db + 'scra.db')

# Tempo de execução do programa:
start_time = time.time()

# Versão do modelo:
model_version = os.getcwd().split('\\')[-1]

# Declara a quantidade de iterações:
n_iterations = [1000]

# Periodos a serem rodados:
periodo = ['2021-10', '2021-11', '2021-12', '2022-01']

# Filtro para ignorar determinados componentes
ignore_list = []

# Iteração para cada período:
for period in periodo:

    # Caddera o Input de produtos (PA)
    df_dem_sim = pd.read_sql('SELECT * FROM Input_PA', conn) # Leitura da tabela de produtos
    df_dem_sim = df_dem_sim[(df_dem_sim['AnoMes'] == period)] # Filtro do período
    df_dem_sim = df_dem_sim.sort_values('Produto') # Ordena por ordem crescente do código de produto
    df_dem_sim.head()

    # Componentes
    df_bom_filt = pd.read_sql('SELECT * FROM Input_Insumos', conn) # Leitura da tabela de insumos
    df_bom_filt = df_bom_filt[(df_bom_filt['AnoMes'] == period)] # Filtro do período
    df_bom_filt = df_bom_filt.sort_values('Produto') # Ordena por ordem crescente do código de produto
    df_bom_filt.head(3)

    df_mat = copy.deepcopy(df_bom_filt)
    df_mat = df_mat.rename(columns = {'COD_COMPONENTE': 'Material'}) # Renomeia coluna componentes
    df_mat = df_mat.dropna(subset = ['OTIF_Min', 'OTIF_Median', 'OTIF_Max', 'M1_Min', 'M1_Median', 'M1_Max']) # Remove fornecedores que não temos info de OTIF
    df_mat.head()

    # Simulação de Monte Carlo

    # Simulação demanda PA
    demanda_real = scra_montecarlo.simulacao_demanda(df_dem_sim, 'Produto', 'Produção', n_iterations)
    demanda_real.head(3)

    # Flag de Stock-Out devido a Demanda (Maior que planejada)
    demanda_real_2 = demanda_real.merge(df_dem_sim[['Produto', 'PV_Baseline', 'PV_Incremental', 'Previsto', 'Produção', 'Estoque_Total_PA', 'Estoque_Seguranca_PA', 'Estoque_Util_PA']], on = 'Produto')
    demanda_real_2['Flag_OverDemand'] = demanda_real_2['Demanda_Real'] > demanda_real_2['Previsto']
    demanda_real_2

    # Simulação disponibilidade (Confiabilidade, Flexibilidade e Ajuste da PV de curto prazo)
    mat_real = scra_montecarlo.simulacao_disponibilidade(df_mat, 'Produto', 'Material')

    # Tratativa de resultados
    # As próximas linhas de código tratam o output das simulações anteriores
    # Algumas convenções importantes validas para todas as tratatias seguintes:
    # Conf: Confiabilidade - Resultado da compra de insumos do fovrnecedor respeitando o lead time acordado
    # Flex: Flexibilidade - Capacidade do fornecedor de reagir e entregar uma quantidade a mais solicitada fora do lead time acordado
    # ConfFlex: Soma da Confiabilidade e Flexibilidade. Soma das duas simulações que representao o total obtido pelo fornecedor naquele período
    # Flag_: Indica um tipo de coluna que faz uma verificação de verdadeiro ou falso. Por exemplo: Stockout: A demanda simulada foi maior que a disponibilidade simulada?

    # Soma o resultado da Confiabilidade e Flexibilidade, criando colunas temporárias para tratar valores nulos
    mat_real = mat_real.merge(demanda_real_2, on = ['Sim', 'Iter', 'Produto']) # Junta resultados da simulação de PA com simulação de insumos
    mat_real['Compras_Final_temp1'] = mat_real.loc[mat_real['Flag_OverDemand'] == True, ['Compras_Conf', 'Compras_Flex']].sum(axis = 1)
    mat_real['Compras_Final_temp2'] = mat_real.loc[mat_real['Compras_Final_temp1'].isna(), 'Compras_Conf']
    mat_real['Compras_Final_temp1'] = mat_real['Compras_Final_temp1'].fillna(0)
    mat_real['Compras_Final_temp2'] = mat_real['Compras_Final_temp2'].fillna(0)
    mat_real['Compras_Conf_Flex'] = mat_real['Compras_Final_temp1'] + mat_real['Compras_Final_temp2']
    mat_real = mat_real.drop(labels = ['Compras_Final_temp1', 'Compras_Final_temp2'], axis = 1)

    # Traz informações de previsão e produção do insumo
    df_mat_prev = df_mat[['Produto', 'Material', 'Produção_Mat', 'Previsão_Mat']]
    mat_real = mat_real.merge(df_mat_prev, on = ['Produto', 'Material'])
    # Colunas Flag indicando compras abaixo da meta estabelecida
    mat_real['Flag_UnderPurchase_Conf'] = mat_real['Compras_Conf'] < mat_real['Compras_Meta_Mat']
    mat_real['Flag_UnderPurchase_Flex'] = mat_real['Compras_Flex'] < mat_real['Compras_Meta_Mat']
    mat_real['Flag_UnderPurchase_ConfFlex'] = mat_real['Compras_Conf_Flex'] < mat_real['Compras_Meta_Mat']

    # Traz informações de estoque do insumo
    df_mat_est = df_mat[['Produto', 'Material', 'AnoMes', 'Estoque_Total_Mat', 'Estoque_Seguranca_Mat', 'Estoque_Util_Mat']]
    mat_real = mat_real.merge(df_mat_est, on = ['Produto', 'Material', 'AnoMes'])
    mat_real['Estoque_Conf'] = mat_real['Compras_Conf'] + mat_real['Estoque_Total_Mat']
    mat_real['Estoque_Flex'] = mat_real['Compras_Flex'] + mat_real['Estoque_Total_Mat']
    mat_real['Estoque_Conf_Flex'] = mat_real['Compras_Conf_Flex'] + mat_real['Estoque_Total_Mat']
    # Colunas Flag informando se o estoque disponível é inferior ao necessário para produção
    mat_real['Flag_UnderInv_Mat_Conf'] = mat_real['Estoque_Conf'] < mat_real['Produção_Mat']
    mat_real['Flag_UnderInv_Mat_Flex'] = mat_real['Estoque_Flex'] < mat_real['Produção_Mat']
    mat_real['Flag_UnderInv_Mat_ConfFlex'] = mat_real['Estoque_Conf_Flex'] < mat_real['Produção_Mat']
    # Verifica se o estoque de segurança preciso ser utilizado
    mat_real['Flag_UnderSafetyInv_Mat_Conf'] = mat_real['Estoque_Conf'] < mat_real['Produção_Mat']
    mat_real['Flag_UnderSafetyInv_Mat_Flex'] = mat_real['Estoque_Flex'] < mat_real['Produção_Mat']
    mat_real['Flag_UnderSafetyInv_Mat_ConfFlex'] = mat_real['Estoque_Conf_Flex'] < mat_real['Produção_Mat']

    # Traz informação da proporção de insumo para converter em produto acabado
    df_mat_fac = df_mat[['Produto', 'Material', 'FATOR_COMP_PA']]
    mat_real = mat_real.merge(df_mat_fac, on = ['Produto', 'Material'])
    # Conversão dos estoques de insumos em PA - Máximo disponível usando todo o estoque
    mat_real['Estoque_MaxPA'] = round(mat_real['Estoque_Total_Mat'] / mat_real['FATOR_COMP_PA'], 2)
    mat_real['MatConf_MaxPA'] = round(mat_real['Estoque_Conf'] / mat_real['FATOR_COMP_PA'], 2)
    mat_real['MatFlex_MaxPA'] = round(mat_real['Estoque_Flex'] / mat_real['FATOR_COMP_PA'], 2)
    mat_real['MatConfFlex_MaxPA'] = round(mat_real['Estoque_Conf_Flex'] / mat_real['FATOR_COMP_PA'], 2)

    # Aviso: Parte mais lenta para rodar do código: Operação group by muito lenta
    # Caminho alternativo: Adicionar operação "apply "na função
    # Identificação do valor mínimo de cada iteração pro produto, mostrando o máximo que pode ser produzido de PA definido pelo insumo gargalo
    df_mat_iterpa = mat_real.groupby(['Model', 'Sim', 'Iter', 'Produto'], as_index = False).min()
    df_mat_iterpa = df_mat_iterpa[['Model', 'Sim', 'Iter', 'Produto', 'Estoque_MaxPA', 'MatConf_MaxPA', 'MatFlex_MaxPA', 'MatConfFlex_MaxPA']]
    df_mat_iterpa = df_mat_iterpa.rename(columns = {'Estoque_MaxPA': 'Iter_Estoque_MaxPA', 'MatConf_MaxPA': 'Iter_Conf_MaxPA', 'MatConfFlex_MaxPA': 'Iter_ConfFlex_MaxPA', 'MatFlex_MaxPA': 'Iter_Flex_MaxPA'})
    mat_real = mat_real.merge(df_mat_iterpa, on = ['Model', 'Sim', 'Iter', 'Produto'])

    # Traz informação de margem bruta unitária por PA
    mat_real = mat_real.merge(df_dem_sim[['Produto', 'MB_Unit']], on = ['Produto'])

    # Indicador se foi necessário utilizar o estoque de segurança do produto acabado
    mat_real['Flag_UnderSafetyInv_PA_Estoque'] = mat_real['Iter_Estoque_MaxPA'] < mat_real['Estoque_Util_PA']
    mat_real['Flag_UnderSafetyInv_PA_Conf'] = mat_real['Iter_Conf_MaxPA'] < mat_real['Estoque_Util_PA']
    mat_real['Flag_UnderSafetyInv_PA_Flex'] = mat_real['Iter_Flex_MaxPA'] < mat_real['Estoque_Util_PA']
    mat_real['Flag_UnderSafetyInv_PA_ConfFlex'] = mat_real['Iter_ConfFlex_MaxPA'] < mat_real['Estoque_Util_PA']

    # Estoque total disponível do produto
    mat_real['PA_Disponivel_Estoque'] = mat_real['Iter_Estoque_MaxPA'] + mat_real['Estoque_Total_PA']
    mat_real['PA_Disponivel_Conf'] = mat_real['Iter_Conf_MaxPA'] + mat_real['Estoque_Total_PA']
    mat_real['PA_Disponivel_Flex'] = mat_real['Iter_Flex_MaxPA'] + mat_real['Estoque_Total_PA']
    mat_real['PA_Disponivel_ConfFlex'] = mat_real['Iter_ConfFlex_MaxPA'] + mat_real['Estoque_Total_PA']

    # Para casos em que a disponibilidade for maior que a necessidade, ajusta para produzir apenas o necessário. Seja da demanda simulada ou da produção
    mat_real['PA_Disponivel_MaxProd'] = mat_real['PA_Disponivel_ConfFlex']
    mat_real.loc[mat_real['PA_Disponivel_ConfFlex'] > mat_real['Demanda_Real'], 'PA_Disponivel_MaxProd'] = np.maximum(mat_real['Demanda_Real'], mat_real['Produção'])

    # Flag identificando se o estoque disponível é inferior a demanda original prevista do PA
    mat_real = mat_real.rename(columns = {'Previsto': 'PA_Previsto'})
    mat_real['Previsto'] = mat_real['PA_Previsto']
    mat_real = mat_real.drop(labels = 'Previsto', axis = 1)
    mat_real['Flag_UnderForecastPA_Estoque'] = mat_real['PA_Disponivel_Estoque'] < mat_real['PA_Previsto']
    mat_real['Flag_UnderForecastPA_Conf'] = mat_real['PA_Disponivel_Conf'] < mat_real['PA_Previsto']
    mat_real['Flag_UnderForecastPA_Flex'] = mat_real['PA_Disponivel_Flex'] < mat_real['PA_Previsto']
    mat_real['Flag_UnderForecastPA_ConfFlex'] = mat_real['PA_Disponivel_ConfFlex'] < mat_real['PA_Previsto']

    # Identifica stockout do produto, comparando disponibilidade simulada com demanda simulada
    mat_real['Stockout_Estoque'] = mat_real['PA_Disponivel_Estoque'] < mat_real['Demanda_Real']
    mat_real['Stockout_Conf'] = mat_real['PA_Disponivel_Conf'] < mat_real['Demanda_Real']
    mat_real['Stockout_Flex'] = mat_real['PA_Disponivel_Flex'] < mat_real['Demanda_Real']
    mat_real['Stockout'] = mat_real['PA_Disponivel_ConfFlex'] < mat_real['Demanda_Real']

    # Calcula o volume de stockout. Valores positivos representam falta de produto enquanto negativos mostram que há itens em estoque
    mat_real['Stockout_Vol_Estoque'] = mat_real['Demanda_Real'] - mat_real['PA_Disponivel_Estoque']
    mat_real['Stockout_Vol_Conf'] = mat_real['Demanda_Real'] - mat_real['PA_Disponivel_Conf']
    mat_real['Stockout_Vol_Flex'] = mat_real['Demanda_Real'] - mat_real['PA_Disponivel_Flex']
    mat_real['Stockout_Vol'] = mat_real['Demanda_Real'] - mat_real['PA_Disponivel_ConfFlex']

    # Calculo da Margem at Risk
    mat_real = mat_real.rename(columns = {'Demanda_Real': 'PA_Real'})
    mat_real['MarginAtRisk_Estoque'] = mat_real['Stockout_Vol_Estoque'] * mat_real['MB_Unit']
    mat_real['MarginAtRisk_Conf'] = mat_real['Stockout_Vol_Conf'] * mat_real['MB_Unit']
    mat_real['MarginAtRisk_Flex'] = mat_real['Stockout_Vol_Flex'] * mat_real['MB_Unit']
    mat_real['MarginAtRisk'] = mat_real['Stockout_Vol'] * mat_real['MB_Unit']
    mat_real.head()

    # Traz informações de  acordos logícos
    df_acordos = copy.deepcopy(df_bom_filt)
    df_acordos = df_acordos[['Produto', 'COD_COMPONENTE', 'COD_FORNECEDOR', 'LEAD_TIME_CADASTRADO', 'LEAD_TIME_ACORDO', 'TIPO_ACORDO', 'VOLUME_ACORDADO', 'Acordo_Logistico']]
    df_acordos = df_acordos.drop_duplicates()
    df_acordos = df_acordos.rename(columns = {'COD_PA': 'Produto'})

    df_acordos = df_acordos.rename(columns = {'COD_COMPONENTE': 'Material', 'COD_FORNECEDOR': 'Cod_Fornecedor',
                                                'LEAD_TIME_CADASTRADO': 'LEAD_TIME_CADASTRADO_','LEAD_TIME_ACORDO': 'LEAD_TIME_ACORDO_', 'TIPO_ACORDO': 'TIPO_ACORDO_', 
                                                'VOLUME_ACORDADO': 'VOLUME_ACORDADO_', 'Acordo_Logistico': 'Acordo_Logistico_'})

    col_list = ['Acordo_Logistico_', 'VOLUME_ACORDADO_', 'TIPO_ACORDO_', 'LEAD_TIME_ACORDO_', 'LEAD_TIME_CADASTRADO_']

    mat_real = mat_real.merge(df_acordos, on = ['Produto', 'Material', 'Cod_Fornecedor'], how = 'left')

    for col in col_list:
        mat_real.insert(8, col[:-1], mat_real[col])
        mat_real = mat_real.drop(col, 1)
    mat_real.head()

    # Resultado por PA
    # Versão resumida, considerando apenas os produtos, sem entrar no detalhe de insumo
    # Nesta versão, considera-se o máximo que poderia ser produzindo baseado no insumo gargalo
    pa_real = copy.deepcopy(mat_real)
    pa_real = pa_real[['Model', 'Sim', 'Iter', 'AnoMes', 'Produto', 'ComprasExtras_PA', 'Iter_Estoque_MaxPA', 'Iter_Conf_MaxPA', 'Iter_Flex_MaxPA', 'Iter_ConfFlex_MaxPA', 'Estoque_Total_PA', 'Estoque_Seguranca_PA', 'Estoque_Util_PA', 'PA_Disponivel_Estoque', 'PA_Disponivel_Conf', 'PA_Disponivel_Flex', 'PA_Disponivel_ConfFlex', 'PA_Disponivel_MaxProd', 'Produção', 'PA_Previsto', 'PV_Baseline', 'PV_Incremental', 'Flag_UnderForecastPA_Estoque', 'Flag_UnderForecastPA_Conf', 'Flag_UnderForecastPA_Flex', 'Flag_UnderForecastPA_ConfFlex', 'Distrib_Dem', 'PA_Real', 'Flag_OverDemand', 'Stockout_Estoque', 'Stockout_Conf', 'Stockout_Flex','Stockout', 'Stockout_Vol_Estoque', 'Stockout_Vol_Conf', 'Stockout_Vol_Flex', 'Stockout_Vol','MB_Unit', 'MarginAtRisk_Estoque', 'MarginAtRisk_Conf', 'MarginAtRisk_Flex','MarginAtRisk']]
    pa_real = pa_real.drop_duplicates()
    pa_real.head()

    # Traz informações dos produtos, como família, categoria e descrição
    df_desc = pd.read_sql('SELECT * FROM Produtos_Desc', conn)

    df_desc_pa = copy.deepcopy(df_desc)
    df_desc_pa = df_desc_pa[['COD_MATERIAL', 'DES_MATERIAL', 'DES_UN_MATERIAL', 'DES_SUBCATEGORIA_MATERIAL']]
    df_desc_pa = df_desc_pa.rename(columns = {'COD_MATERIAL': 'Material', 'DES_MATERIAL': 'DES_PA'})
    df_desc_pa

    # Traz o nome resumido do produto
    df_escopo = pd.read_sql('SELECT * FROM Produtos_Escopo', conn)
    df_escopo = df_escopo[['Material', 'Nome_Resumido']]

    df_desc_pa = df_desc_pa.merge(df_escopo, on = 'Material')
    df_desc_pa = df_desc_pa.rename(columns = {'Material': 'Produto'})
    df_desc_pa.head(3)

    df_desc_ins = copy.deepcopy(df_desc)
    df_desc_ins = df_desc_ins[['COD_MATERIAL', 'DES_MATERIAL', 'DES_FAMILIA', 'DES_TIPO_MATERIAL']]
    df_desc_ins = df_desc_ins.rename(columns = {'COD_MATERIAL': 'Material', 'DES_MATERIAL': 'DES_COMPONENTE'})

    df_desc_ins.head(3)

    mat_real = mat_real.merge(df_desc_pa, on = 'Produto', how = 'left')
    mat_real = mat_real.merge(df_desc_ins, on = 'Material', how = 'left')
    mat_real.head(3)

    pa_real = pa_real.merge(df_desc_pa, on = 'Produto', how = 'left')
    pa_real.head(3)

    # Salva os resultado em CSV, na pasta do período executado
    mat_real.to_csv(period + '//Materials_Results' + period + '.csv', index = False)
    pa_real.to_csv(period + '//Products_Results' + period + '.csv', index = False)

# Tempo de execução do programa
finish_time = round(time.time() - start_time, 0)
print("--- {} seconds ---".format(finish_time))
print("--- {} minutes ---".format(round(finish_time / 60, 0)))