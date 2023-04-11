# Importa bibliotecas
import os
import time
import numpy as np
import pandas as pd
import copy
import sqlite3 as sql
import datetime as dt
import scradb
import variables

# Aumenta quantidade máxima de colunas do Pandas
pd.options.display.max_columns = 99

# Marca tempo de início da execução do programa:
start_time = time.time()

# Conexão com SQLite
conn = sql.connect('scra.db')

# Carrega inf de descrição material
desc_mat = pd.read_csv(variables.path_prod + 'DESC_MATERIAL_20210728.csv', sep=';', decimal = ',', dtype = str)
desc_mat_simple = desc_mat[['COD_MATERIAL', 'DES_MATERIAL']]
desc_mat.head(2)

# Escopo Fase 2
df_escopo = pd.read_excel(variables.path_prod + 'NSO_JUL_2021.xlsx', sheet_name = 'Escopo_SCRA', dtype = str)
df_escopo.tail(2)
lista_escopo = list(df_escopo['Material'].unique()) # Cria lista com SKUs do escopo

# Agrupadores
df_agrup = df_escopo[['Agrup', 'Descrição']].merge(desc_mat[['COD_MATERIAL','DES_MATERIAL', 'COD_AGRUP_DEM_MATERIAL']], left_on = 'Agrup', right_on = 'COD_AGRUP_DEM_MATERIAL', how = 'left')
df_agrup = df_agrup.rename(columns = {'Descrição': 'Descricao_Agrup'})
del df_agrup['COD_AGRUP_DEM_MATERIAL']

# Agrupadores BRAND
df_agrup_eud = pd.read_excel(variables.path_prod + 'Agrupadores_BRAND.xlsx', dtype = str)
df_agrup = df_agrup.merge(df_agrup_eud[['Cód Agrupador', 'Sku', 'Desc Sku']], left_on = 'Agrup', right_on = 'Cód Agrupador', how = 'left')
df_agrup['COD_MATERIAL'] = df_agrup['COD_MATERIAL'].fillna(df_agrup['Sku'])
df_agrup['DES_MATERIAL'] = df_agrup['DES_MATERIAL'].fillna(df_agrup['Desc Sku'])
df_agrup = df_agrup.drop(labels = ['Cód Agrupador', 'Sku', 'Desc Sku'], axis = 1)
df_agrup.head(2)

# Lista Técnica (BOM)
bom = pd.read_csv(variables.path_prod + 'LISTA_TECNICA_20210729.csv', dtype = str)

bom_columns = []
for col in bom.columns:
    bom_columns.append(col.upper())
bom.columns = bom_columns

# Inclui descrição de PA e insumo
desc_mat_pa = desc_mat[['COD_MATERIAL', 'DES_MATERIAL']].drop_duplicates()
desc_mat_pa = desc_mat_pa.rename(columns = {'COD_MATERIAL': 'COD_PA'}) # , 'DES_MATERIAL': 'DES_PA'})
bom = bom.merge(desc_mat_pa, on = 'COD_PA', how = 'left')
bom.insert(loc = 1, column = 'DES_PA', value = bom['DES_MATERIAL'])
bom = bom.drop(labels = ['DES_MATERIAL'], axis = 1)

desc_mat_comp = desc_mat[['COD_MATERIAL', 'DES_MATERIAL']].drop_duplicates()
desc_mat_comp = desc_mat_comp.rename(columns = {'COD_MATERIAL': 'COD_COMPONENTE'}) # , 'DES_MATERIAL': 'DES_COMPONENTE'})
bom = bom.merge(desc_mat_comp, on = 'COD_COMPONENTE', how = 'left')
bom.insert(loc = 12, column = 'DES_COMPONENTE', value = bom['DES_MATERIAL'])
bom = bom.drop(labels = ['DES_MATERIAL'], axis = 1)

bom = bom.rename(columns = {'COD_UMB_MATERIAL': 'COD_UMB_PA', 'QT_BASICA_MATERIAL': 'QT_BASICA_PA'})

bom['QT_COMPONENTE'] = bom['QT_COMPONENTE'].str.replace(',', '.').astype(float)
bom['QT_BASICA_PA'] = bom['QT_BASICA_PA'].astype(float)

bom.head(2)

# Site Principal de Produção
df_prod = pd.read_csv(variables.path_plant + "Planos de Produção Consolidado - Jul'21.csv", skiprows = 6, dtype = {'Código': str}, encoding = 'latin1')
df_prod = df_prod[['Código', 'Descrição', 'Site', 'Fábrica', 'Tipo \nIndustrialização', 'Recurso/Fornecedor', 'Total 2021']]
df_prod = df_prod.rename(columns = {'Código': 'COD_MATERIAL', 'Descrição': 'DES_MATERIAL', 'Total 2021': 'Volume_Producao'})

# Pivota base de produção
df_prod['Site'] = df_prod['Site'].replace('local', 'LOCAL')
df_prod_agg = df_prod.groupby(['COD_MATERIAL', 'DES_MATERIAL', 'Site'], as_index = False).sum()
df_prod_piv = df_prod_agg.pivot(index = ['COD_MATERIAL', 'DES_MATERIAL'], columns = 'Site', values = 'Volume_Producao').reset_index(drop = False)
df_prod_piv_common = df_prod_piv[(df_prod_piv['LOCAL'] > 0) & (df_prod_piv['LOCAL2'] > 0)]
list_common = df_prod_piv_common['COD_MATERIAL'].unique()
df_prod_agg[df_prod_agg['COD_MATERIAL'] == list_common[0]]

# Informação do site produzido em colunas
df_prod_piv_common_agg = df_prod_piv.groupby(['COD_MATERIAL', 'DES_MATERIAL'], as_index = False).sum()
id_max = df_prod_piv_common_agg[['LOCAL', 'LOCAL2', 'TERCEIROS']].values.argmax(axis = 1)
df_prod_piv_common_agg['Producao_Principal'] = [df_prod_piv_common_agg.columns[i + 2] for i in id_max]
df_prod_piv_common_agg = df_prod_piv_common_agg.replace({'Producao_Principal': {'LOCAL': 'BB01', 'LOCAL2': 'BA01', 'TERCEIROS': 'BA01'}})
df_prod_piv_common_agg.head()

# Lista Técnica Principal
bom_simp = copy.deepcopy(bom)

# Filtro dos tipos de componentes do escopo
valid_comp = ['ROH', 'ZEMB', 'ZMEA']
bom_simp = bom_simp[bom_simp['COD_TIPO_COMPONENTE'].isin(valid_comp)]

# Informação Site Produtivo Princial
bom_simp = bom_simp.merge(df_prod_piv_common_agg[['COD_MATERIAL', 'Producao_Principal']], left_on = 'COD_PA', right_on = 'COD_MATERIAL', how = 'left').drop(labels = 'COD_MATERIAL', axis = 1)
bom_simp['Flag_Centro'] = bom_simp['COD_CENTRO'] == bom_simp['Producao_Principal']
bom_simp = bom_simp[bom_simp['Flag_Centro'] == True]
bom_simp.head(2)

# Lead Time Fornecedores
# Carrega informação de lead times dos fornecedores
df_lt = pd.read_excel(variables.path_forn + 'LEAD_TIME_FORNECEDORES_20210729.xlsx', dtype = str)
for col in ['dt_inicio_val_doc_compras', 'dt_fim_val_doc_compras']:
    df_lt[col] = df_lt[col].apply(lambda x: x[:-9])

lt_columns = []
for col in df_lt.columns:
    lt_columns.append(col.upper())
df_lt.columns = lt_columns

df_lt['QT_LEAD_TIME'] = df_lt['QT_LEAD_TIME'].astype(int)
df_lt = df_lt.rename(columns = {'QT_LEAD_TIME': 'LEAD_TIME'})

# Considera apenas o fornecedor mais recente cadastrado da lista
df_lt = df_lt.sort_values('LEAD_TIME', ascending = False)
df_lt = df_lt.sort_values('DT_FIM_VAL_DOC_COMPRAS', ascending = False)
df_lt = df_lt.drop_duplicates(subset = ['COD_MATERIAL'])
df_lt.head()

df_lt_simp = df_lt[['COD_MATERIAL', 'COD_FORNECEDOR', 'LEAD_TIME']]
df_lt_simp = df_lt_simp.rename(columns = {'COD_MATERIAL': 'COD_COMPONENTE'})

# Correção de código de fornecedores: Remover 0 a esquerda para ficar de acordo com outras bases de dados
# O correto é ter zero a esquerda nos códigos de fornecedores: Para esta correção será necessário atualizar as bases de OTIF
df_lt_simp['COD_FORNECEDOR'] = df_lt_simp['COD_FORNECEDOR'].astype(int).astype(str)
df_lt_simp.head()

bom_simp_lt = bom_simp.merge(df_lt_simp, on = ['COD_COMPONENTE'], how = 'left')

# Junta informação de lead time em BOM
bom_simp_lt = bom_simp.merge(df_lt_simp, on = ['COD_COMPONENTE'], how = 'left')
bom_simp_lt = bom_simp_lt.drop_duplicates()
bom_simp_lt.head(2)

# Acordos logísticos
df_acordos = pd.read_excel(variables.path_forn + 'Acordos Ativos.xlsx')
df_acordos = df_acordos[['CÓD FORN', 'SKU', 'LT REDUZIDO', 'TIPO DE ACORDO', 'VOLUME ACORDADO']]
df_acordos = df_acordos.sort_values('LT REDUZIDO')
df_acordos = df_acordos.drop_duplicates(subset = ['CÓD FORN', 'SKU'])
df_acordos = df_acordos.rename(columns = {'CÓD FORN': 'COD_FORNECEDOR', 'SKU': 'COD_COMPONENTE'})
df_acordos = df_acordos.drop_duplicates()
for col in ['COD_FORNECEDOR', 'COD_COMPONENTE']:
    df_acordos[col] = df_acordos[col].astype(str).str.strip()
df_acordos.head(3)

bom_simp_lt = bom_simp_lt.merge(df_acordos, on = ['COD_COMPONENTE', 'COD_FORNECEDOR'], how = 'left')
bom_simp_lt['Acordo_Logistico'] = 'Não'
bom_simp_lt.loc[~bom_simp_lt['LT REDUZIDO'].isna(), 'Acordo_Logistico'] = 'Sim'
# bom_simp_lt.loc[bom_simp_lt['Acordo_Logistico'] == 'Não', 'LT REDUZIDO'] = bom_simp_lt['LEAD_TIME']
bom_simp_lt = bom_simp_lt.rename(columns = {'LEAD_TIME': 'LEAD_TIME_CADASTRADO', 'LT REDUZIDO': 'LEAD_TIME_ACORDO', 'TIPO DE ACORDO': 'TIPO_ACORDO', 'VOLUME ACORDADO': 'VOLUME_ACORDADO'})
bom_simp_lt.head(3)


# Componentes Desconsiderados
# Carrega lista de componentes a serem desconsiderados
# Caso novos componentes tenham que ser desconsiderados, incluir no arquivo abaixo
df_mat_nok = pd.read_excel(variables.path_prod + 'SCRA_VIS_Lista Componentes Desconsiderados.xlsx', dtype = str)
df_mat_nok.tail(2)

# Remove componentes desconsiderados da BOM final
bom_simp_lt = bom_simp_lt[~bom_simp_lt['COD_COMPONENTE'].isin(df_mat_nok['COD_MATERIAL'])]
bom_simp_lt.head(2)

# De-Para Componentes
# Carrega listas de de-para com correspondências entre insumos
# Lista de De-Paras padrão fornecida pela empresa
depara_comp = pd.read_excel(variables.path_prod + 'DE_PARA_MATERIAL_20210806.xlsx', dtype = str)
depara_comp = depara_comp.rename(columns = {'COD_MATERIAL_DE': 'COD_COMPONENTE'})
depara_comp.head()

# Lista de De-Paras manual
# Caso seja necessário alterar o código de algum produto, incluir no arquivo abaixo
depara_comp_manual = pd.read_excel(variables.path_prod + 'SCRA_VIS_DePara_Componentes.xlsx', dtype = str)
depara_comp_manual = depara_comp_manual.rename(columns = {'COD_MATERIAL_DE': 'COD_COMPONENTE'})
depara_comp_manual.head()

depara_comp = pd.concat([depara_comp, depara_comp_manual])

# Realiza a troca de códigos usados na tabela de-para
bom_simp_lt = bom_simp_lt.merge(depara_comp, on = 'COD_COMPONENTE', how = 'left')
bom_simp_lt.loc[~bom_simp_lt['COD_MATERIAL_PARA'].isna(), 'COD_COMPONENTE'] = bom_simp_lt.loc[~bom_simp_lt['COD_MATERIAL_PARA'].isna(), 'COD_MATERIAL_PARA']
bom_simp_lt = bom_simp_lt.drop(labels = 'COD_MATERIAL_PARA', axis = 1)
bom_simp_lt = bom_simp_lt.drop_duplicates()

# BOM Principal Escopo
bom_simp_lt_escopo = bom_simp_lt[bom_simp_lt['COD_PA'].isin(lista_escopo)]

# Exporta Dados
scradb.table_to_sqlite(desc_mat, 'Produtos_Desc')
desc_mat.to_csv(variables.path_db + variables.csv + 'Produtos_Desc.csv', index = False)

scradb.table_to_sqlite(df_escopo, 'Produtos_Escopo')
df_escopo.to_csv(variables.path_db + variables.csv + 'Produtos_Escopo.csv', index = False)

scradb.table_to_sqlite(bom, 'Produtos_ListaTecnica_Full')
bom.to_csv(variables.path_db + variables.csv + 'Produtos_ListaTecnica_Full.csv', index = False)

scradb.table_to_sqlite(bom_simp_lt, 'Produtos_ListaTecnica_Principal')
bom_simp_lt.to_csv(variables.path_db + variables.csv + 'Produtos_ListaTecnica_Principal.csv', index = False)

scradb.table_to_sqlite(bom_simp_lt_escopo, 'Produtos_ListaTecnica_Principal_Escopo')
bom_simp_lt_escopo.to_csv(variables.path_db + variables.csv + 'Produtos_ListaTecnica_Principal_Escopo.csv', index = False)

scradb.table_to_sqlite(df_prod_piv_common_agg, 'Produção_SitePrincipal')
df_prod_piv_common_agg.to_csv(variables.path_db + variables.csv + 'Produção_SitePrincipal.csv', index = False)

scradb.table_to_sqlite(df_lt, 'Fornecedores_LeadTime')
df_lt.to_csv(variables.path_db + variables.csv + 'Fornecedores_LeadTime.csv', index = False)

scradb.table_to_sqlite(df_agrup, 'Produtos_Agrupadores')
df_agrup.to_csv(variables.path_db + variables.csv + 'Produtos_Agrupadores.csv', index = False)

# Tempo de execução do programa:
finish_time = round(time.time() - start_time, 0)
print("--- {} seconds ---".format(finish_time))
print("--- {} minutes ---".format(round(finish_time / 60, 0)))