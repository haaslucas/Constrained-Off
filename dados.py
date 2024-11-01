import pandas as pd
import os
import numpy as np
import locale
locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')

def read_csv():
    dfs = [] #Lista base contendo todos os DataFrames
    base_dir = os.path.dirname(os.path.abspath(__file__)) #Extrai como base o diretório onde o script é executado
    arquivos = os.path.join(base_dir, 'Arquivos', 'Fotovoltaica') #Busca todos os arquivos da pasta 'Fotovoltaica'
    for arquivo in os.listdir(arquivos): #Varre todos os arquivos da pasta
        if arquivo.endswith('.csv'):
            caminho_arquivo = os.path.join(arquivos, arquivo)
            df = pd.read_csv(caminho_arquivo, sep=';') #Arquivos da ons estão separados por ;
            dfs.append(df) #Armazena todos os arquivos .cvs na lista dfs
    df = pd.concat(dfs, ignore_index=True) #Concatena todos os DataFrames em um único
    
    #Separando dia e hora em 2 colunas diferentes
    df['din_instante'] = pd.to_datetime(df['din_instante']) #Conversão em formato de data
    df['Dia'] = df['din_instante'].dt.date #Criação da coluna de dia 
    df['Hora'] = df['din_instante'].dt.time #Criação da coluna de tempo
    df['Mes'] = pd.to_datetime(df['Dia']).dt.strftime('%b %Y')
    # Preenchimento do Corte %, relacionando a geração limitada a disponibilidade
    df['Corte %'] = np.where(
        (df['val_geracaolimitada'].notna()) & 
        (df['val_disponibilidade'].notna()) & 
        (df['val_disponibilidade'] != 0) & 
        (df['val_geracaolimitada'] <= df['val_disponibilidade']),  # Verificação adicional
        100 * (df['val_geracaolimitada'] / df['val_disponibilidade']),  # Corte %
        np.nan  # Caso contrário, atribui NaN
    )
    df['Geracao MWh'] = df['val_geracao']/2 #Geração em MWh é a geração média (dada em intervalos de 30 min) / 2
    df['Geracao limitada MWh'] = df['val_geracaolimitada']/2
    return df

df = read_csv()