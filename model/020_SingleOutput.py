# Importa bibliotecas
import os
import time
import pandas as pd

# Marca tempo de início da execução do programa:
start_time = time.time()

# Carrega lista de arquivos no diretório
list_per = []
for item in os.listdir():
    if '.' in item:
        continue
    if 'Old' in item:
        continue
    if '__' in item:
        continue
    list_per.append(item)

# Define função para consolidar arquivos excel (não utilizada atualmente)
def consolidate_excel(file, list_per = list_per):
    df_consol = pd.DataFrame()
    for per in list_per:
        df = pd.read_excel(per + '//' + file, dtype = {'Produto': str, 'Material': str})
        df_consol = pd.concat([df_consol, df])
    
    df_consol.to_excel(file, index = False)
    
    return None

# Define função para consolidar arquivos csv
def consolidate_csv(file, list_per = list_per):
    df_consol = pd.DataFrame()
    for per in list_per:
        df = pd.read_csv(per + '//' + file + per + '.csv', dtype = {'Produto': str, 'Material': str})
        df_consol = pd.concat([df_consol, df])
    
    df_consol.to_csv(file + '.csv', index = False)
    
    return None

# Lista de nome de arquivos csv
csv_files = ['Materials_Results', 'Products_Results']

# Executa função de consolidação de CSVs para lista informada
d_csv = {}
for csv in csv_files:
    d_csv[csv] = consolidate_csv(csv)

finish_time = round(time.time() - start_time, 0)
print("--- {} seconds ---".format(finish_time))
print("--- {} minutes ---".format(round(finish_time / 60, 0)))