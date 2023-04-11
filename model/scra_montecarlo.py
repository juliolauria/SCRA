import os
import numpy as np
import pandas as pd
from tqdm import tqdm
from scipy import stats

# Versão do modelo:
model_version = os.getcwd().split('\\')[-1]

# Declara a quantidade de iterações:
n_iterations = [1000]

# Filtro para ignorar determinados componentes
ignore_list = []

# Simulação Demanda

def simulacao_demanda(df, mat, mean, n_iter = n_iterations):
    """
    Função para simulação da demanda de produtos.
    Input: DataFrame de input, colunas indicando o produto, média, e quantidade de iterações
    Output: DataFrame com resultados da simulação
    """
    df_result = pd.DataFrame() # DataFrame de resultados, onde será inserido os resultado para cada produto 
    # Realizada simulações em função da quantidade de iterações informadas
    for i, n_i in enumerate(n_iter):
        # Execução para cada PA da lista:
        for m in tqdm(df[mat]):
            # Exclui determinado PA caso informado na lista
            if m in ignore_list:
                continue
            else:
                # DataFrame de filtro temporário contendo apenas info do PA da iteração
                df_filt = df[df[mat] == m]
                df_temp = pd.DataFrame()
                np.random.seed(7)

                # Parâmetros para distribuição normal truncada
                a = df_filt['Prev_Min'].values[0] # Valor mínimo
                b = min(df_filt['Prev_Max'].values[0], 2) # Valor máximo: O menor valor entre 2x a demanda e a demanda máxima observada no histórico
                mu = 1 # Média / Valor esperado (Será igual a previsão do período)
                sigma = df_filt['Razao_RealPrev_Std_BaseInc'].values[0] # Desvio padrão do input

                # Simulação de valores baseada na distribuição truncada
                arr = stats.truncnorm.rvs(a = ((a - mu) / sigma), # Valor mínimo a ser truncado
                                        b = ((b - mu) / sigma),  # Valor máximo a ser truncado
                                        loc = mu, # Média / Valor Esperado
                                        scale = sigma, # Desvio padrão
                                        size = n_i) # Quantidade de variáveis a serem geradas

                df_temp['Sim'] = ['Sim' + str(i + 1)] * len(arr) # Npumero da Simulação
                df_temp['Iter'] = range(1,len(arr) + 1) # Número da Iteração
                df_temp['Produto'] = [m] * len(arr) # Produto Simulado
                df_temp['Distrib_Dem'] = arr # Distribuição da demanda simulada. Valor no formato razão realizado/previsto
                df_temp['Demanda_Real'] = df_filt['Previsto'].values[0] * arr # Multiplica a demanda prevista pelo valor simulado
                df_result = pd.concat([df_result, df_temp]) # Concatena resultados de outros produtos para consolidar arquivo final
        
    df_result['Demanda_Real'] = df_result['Demanda_Real'].round(2) # Arredonda valores numéricos da simulação

    return df_result

# Simulação Disponibilidade de Insumos

def simulacao_disponibilidade(df, pa, mat, n_iters = n_iterations):
    """
    Função para simulação dos insumos dos produtos.
    No total roda 3 simulações: Ajuste do plano de vendas de curto prazo (Compras extras), confiabilidade e flexibilidade.
    Input: DataFrame de input de insumos, colunas de identificação do produto, insumo e quantidade de iterações
    Output: DataFrame com o resultado das simulações
    """
    df_final = pd.DataFrame()

    # Realizada simulações em função da quantidade de iterações informadas
    for i, n_i in enumerate(n_iters):
        # Execução para cada PA da lista:
        for pa_i in tqdm(df[pa].unique()):
            np.random.seed(7)
            if pa_i in ignore_list:
                continue
            
            # Necessidade extra de compra
            # Filtro de PA
            df_filt_flex = df[(df[pa] == pa_i)]

            # Distribuição triangular de compras extras
            # Tratativa para casos em que valor mínimo e máximo sejam iguais:
            # Adiciona valor constante:
            if df_filt_flex['M1_Min'].values[0] == df_filt_flex['M1_Max'].values[0]:
                arr_dist_compras_extras_pa = np.empty(n_i)
                arr_dist_compras_extras_pa.fill(df_filt_flex['M1_Min'].values[0])

            # Distribuição triangular para compras extras no curto prazo:
            # Distribuição no formato razão realizado/previsto
            else:
                arr_dist_compras_extras_pa = np.random.triangular(df_filt_flex['M1_Min'].values[0], # Mínimo
                                                            df_filt_flex['M1_Median'].values[0], # Valor Esperado
                                                            df_filt_flex['M1_Max'].values[0], # Máximo
                                                            n_i) # Quantidade de iterações
            

            arr_compras_extras_pa = (df_filt_flex['Previsto_PA'].values[0]) * (arr_dist_compras_extras_pa - 1) # Multiplica pelo valor previsto
            arr_compras_extras_pa[arr_compras_extras_pa < 0] = 0 # Valores negativos são considerados como zero

            # Execução para cada insumo do PA:
            for mat_i in df[df[pa] == pa_i][mat].unique():
                if mat_i in ignore_list:
                    continue
                df_temp = pd.DataFrame() # Cria DataFrame temporário com resultados
                df_filt = df[(df[pa] == pa_i) & (df[mat] == mat_i)] # Filtra PA e Insumo
                df_temp['Model'] = [model_version] * n_i # Nome do modelo
                df_temp['Sim'] = ['Sim' + str(i + 1)] * n_i # Número da Simulação
                df_temp['Iter'] =  range(1, n_i + 1) # Número da iteração
                df_temp['AnoMes'] = df_filt['AnoMes'].values[0] # Período avaliado
                df_temp['Produto'] = df_filt[pa].values[0] # Produto (PA)
                df_temp['Material'] = df_filt[mat].values[0] # Insumo (ME/MP)
                df_temp['Cod_Fornecedor'] = df_filt['COD_FORNECEDOR'].values[0] # Código do fornecedor
                df_temp['Fornecedor'] = df_filt['Fornecedor'].values[0] # Descrição do fornecedor
                df_temp['Compras_Meta_Mat'] = df_filt['Compras_Meta_Mat'].values[0] # Meta de compras do insump

                var = 'Compras_Meta_Mat'
                np.random.seed(7)
                
                # Simulação da Confiabilidade:
                
                # Caso valores mínimos e máximos sejam iguais: Utilizar valor constante igual ao mínimo
                if df_filt['OTIF_Min'].values[0] == df_filt['OTIF_Max'].values[0]:
                    df_temp['Distrib_Conf'] = df_filt['OTIF_Min'].values[0]

                # Caso o valor esperado seja igual ao máximo, adicionar um valor pequeno ao máximo para conseguir rodar a distribuição triangular
                elif df_filt['OTIF_Median'].values[0] == df_filt['OTIF_Max'].values[0]:
                    df_temp['Distrib_Conf'] = np.random.triangular(df_filt['OTIF_Min'], # Mínimo
                                                            (df_filt['OTIF_Median']), # Valor Esperado
                                                            df_filt['OTIF_Max'] + 0.01, # Máximo com correção
                                                            n_i) # Quantidade de iterações
                # Caso contrário, execução normal
                else:    
                    df_temp['Distrib_Conf'] = np.random.triangular(df_filt['OTIF_Min'], # Mínimo
                                                            df_filt['OTIF_Median'], # Valor Esperado
                                                            df_filt['OTIF_Max'], # Máximo
                                                            n_i) # Quantidade de iterações

                # Corrige simulações em que o resultado seja superior a 1:                        
                df_temp.loc[df_temp['Distrib_Conf'] > 1, 'Distrib_Conf'] = 1
                
                # Multiplica o resultado da distribuição (em percentual) com o valor nominal de compras
                df_temp['Compras_Conf'] = df_temp['Distrib_Conf'] * df_filt[var].values[0]

                # Flexibilidade:

                # Quantidade extra a ser comprada PA:
                # Simulado anterioremente por PA
                df_temp['Distrib_ComprasExtras'] = arr_dist_compras_extras_pa
                df_temp['ComprasExtras_PA'] = arr_compras_extras_pa
                
                # Quantidade flexibilidade

                df_temp['Flexibilidade'] = [df_filt['Flexibilidade'].values[0]] * n_i # repete valor de flexibilidade para cada iteração
                
                # Tratativa para casos com mesmo valor mínimo e máximo
                if df_filt['Flex_Min'].values[0] == df_filt['Flex_Max'].values[0]:

                    df_temp['Distrib_Flex'] = df_filt['Flex_Min'].values[0] # Valor constante baseado no mpinimo
                
                else:

                    df_temp['Distrib_Flex'] = np.random.triangular(df_filt['Flex_Min'], # Mínimo flexibilidade
                                                                df_filt['Flex_Median'], # Valor esperado flexibilidade
                                                                df_filt['Flex_Max'], # Máximo flexibilidade
                                                                n_i) # Número de iterações


                df_temp['Compras_Meta_Extra_Mat'] = (df_temp['Distrib_ComprasExtras'] - 1) * df_filt['Previsão_Mat'].values[0] # Cálculo da quantidade de compras extras
                df_temp.loc[df_temp['Compras_Meta_Extra_Mat'] < 0, 'Compras_Meta_Extra_Mat'] = 0 # Para compras extras negativas, substitui para zero
                df_temp['Compras_Flex_Max'] = df_temp['Distrib_Flex'] * df_temp['Compras_Meta_Extra_Mat'] # O Máximo de compras extras que pode ter sido comprado pela simulação de flexibilidade. Este valor em alguns casos pode ser superior a demanda
                df_temp['Compras_Flex'] = np.minimum(df_temp['Compras_Meta_Extra_Mat'], df_temp['Compras_Flex_Max']) # Corrige para considerar o menor valor, não adquirindo mais insumos na flexibilidade do que o necessário

                df_final = pd.concat([df_final, df_temp]) # Junta informações de cada produto/insumo
                df_final = df_final.sort_values(['Model', 'Sim', 'Iter']) # Ordena conforme nome do modelo, simulação e iteração

    return df_final 