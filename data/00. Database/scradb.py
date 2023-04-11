# Funções customizadas para preparar base de dados
import numpy as np
import pandas as pd
import sqlite3 as sql
import variables

# path_db = 'PATH'
conn = sql.connect('scra.db')

def month_leading_zero(df, col):
    """
    Adiciona zero à esquerda em uma coluna. 
    Usado especialmente para a coluna "mês".
    Valor zero só será adicionado caso o tamanho do valor seja igual a 1. Ex: 1 -> 01 and 11 -> 11.
    O tipo de dados será convertido para str no final da operação.
    """
    df[col] = df[col].astype(str).apply(lambda x: '0' + x if len(x) == 1 else x)

def anomes(df, ano, mes, col = 'AnoMes'):
    """
    Cria uma nova coluna combinando as colunas 'Ano' e 'Mês', no formato 'YYYY-MM' (Ano-Mês).
    Certifique-se que as colunas 'Ano' e 'Mês" já estejam no formato 'YYYY' (Ex: 2020) e 'MM' (Ex: 09), respectivamente.
    """
    df[col] = df[ano].astype(str) + '-' + df[mes].astype(str)

def table_to_sqlite(df, table_name, db = 'scra.db'):
    """
    Cria uma tabela no banco de dados SQLite3 usando o DataFrame informado.
    A tabela primeiro é excluida caso exista para depois ser criada uma nova tabela no banco de dados.
    """
    conn = sql.connect(db)
    cursor = conn.cursor()
    cursor.execute('DROP TABLE IF EXISTS ' + table_name)
    conn. commit()

    df.to_sql(table_name, conn, index = False)

def agrup(df, on = ['AnoMes', 'Agrup'], con=conn):
    """
    Adiciona o código agrupador equivalente para cada código de SKU e retorna um DataFrame agrupado em função do Agrupador.
    O código do Material no resultado final é o SKU referência do Agrupador, informado na tabela de Escopo
    """
    df_escopo = pd.read_sql('SELECT * FROM Produtos_Escopo', conn)
    df_escopo = df_escopo[['Agrup', 'Material']]

    df_agrup = pd.read_sql('SELECT * FROM Produtos_Agrupadores', conn)
    df_agrup = df_agrup[['COD_MATERIAL', 'Agrup']]

    df = df.merge(df_agrup, on = 'COD_MATERIAL', how = 'left')
    df_agrup = df.drop(labels = 'COD_MATERIAL', axis = 1)
    df_agrup = df_agrup.groupby(on, as_index = False).sum()

    df_agrup = df_agrup.merge(df_escopo, on = 'Agrup')
    material = df_agrup['Material']
    df_agrup = df_agrup.drop(labels = 'Material', axis = 1)
    df_agrup.insert(df_agrup.columns.get_loc('Agrup') + 1, 'Material', material)

    return df, df_agrup

def agrup_escopo(df, con = conn):
    """
    Filtra a lista completa de SKUs para conter apenas os SKUs do escopo, considerando a lista completa de agrupadores
    """

    df_agrup = pd.read_sql('SELECT * FROM Produtos_Agrupadores', conn)
    escopo_list = df_agrup['COD_MATERIAL'].to_list()
    df = df[df['COD_MATERIAL'].isin(escopo_list)]

    return df

def est_etl(df_est):
    """
    Tratativa da base 'DATABASE' referente a estoque projetado. Tanto para PA quanto para insumos.
    """
    df_est = df_est.rename(columns = {'ESTOQUE_PROJETADO_Sum': 'ESTOQUE_PROJETADO', 'ESTOQUE_SEGURANCA_Sum': 'ESTOQUE_SEGURANCA'})
    df_est['AnoMes'] = df_est['AnoMes'].str.replace(',000000', '')

    df_est['ESTOQUE_PROJETADO'] = df_est['ESTOQUE_PROJETADO'].str.replace(',', '.').astype(float)
    df_est['ESTOQUE_SEGURANCA'] = df_est['ESTOQUE_SEGURANCA'].str.replace(',', '.').astype(float)
    # Cria coluna 'ESTOQUE_UTIL', sendo o estoque projetado menos o estoque de segurança
    df_est['ESTOQUE_UTIL'] = df_est['ESTOQUE_PROJETADO'] - df_est['ESTOQUE_SEGURANCA']

    # df_est['ESTOQUE_SEGURANCA_DISP'] = df_est['ESTOQUE_UTIL'] - df_est['ESTOQUE_SEGURANCA']

    # Estoque Projetado Disponível:
    df_est['ESTOQUE_PROJETADO_POSITIVO'] = [x if x > 0 else 0 for x in df_est['ESTOQUE_PROJETADO']]
    df_est['ESTOQUE_UTIL_POSITIVO'] = [x if x > 0 else 0 for x in df_est['ESTOQUE_UTIL']]

    # Estoque segurança disponível
    est_seg_disp_list = []
    for row in df_est.iterrows():

        if  row[1]['ESTOQUE_PROJETADO'] - row[1]['ESTOQUE_SEGURANCA'] > 0:
            est_seg_disp_list.append(row[1]['ESTOQUE_SEGURANCA'])
        else:
            est_seg_disp_list.append(row[1]['ESTOQUE_PROJETADO_POSITIVO'])

    df_est['ESTOQUE_SEGURANCA_DISP'] = est_seg_disp_list
    
    return df_est

def est_m1_etl(df_m1):
    """
    Tratativa para resumir a base de estoque projetado contendo apenas a informação mais recente da última projeção de estoque
    """
    # df_m1['DataFoto'] = pd.to_datetime(df_m1['DataFoto'], format = '%d/%m/%Y')
    df_m1['DataFoto'] = pd.to_datetime(df_m1['DataFoto'], format = '%d-%m-%Y')
    df_m1['AnoMes'] = pd.to_datetime(df_m1['AnoMes'].str.replace(',000000', '').astype(str), format = '%Y%m')
    df_m1['D'] = (df_m1['AnoMes'] - df_m1['DataFoto']) / np.timedelta64(1, 'D')
    df_m1['M'] = round((df_m1['AnoMes'] - df_m1['DataFoto']) / np.timedelta64(1, 'M'), 0)
    df_m1 = df_m1.sort_values('DataFoto', ascending = False)
    df_m1 = df_m1.drop_duplicates(subset = ['Material', 'AnoMes'])
    df_m1_agg = df_m1.groupby(['Material', 'AnoMes', 'M'], as_index = False).sum().drop('D', 1)
    df_m1_agg['AnoMes'] = df_m1_agg['AnoMes'].astype(str).apply(lambda x: x[:7])
    
    return df_m1_agg

