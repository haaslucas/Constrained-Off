import pandas as pd
import os
import numpy as np
import locale
locale.setlocale(locale.LC_TIME, 'pt_BR.UTF-8')
from download_coff import DownloadManager

manager = DownloadManager()
manager.download_files()
manager.download_filesEOL()

def read_csv():
    dfs = [] #Lista base contendo todos os DataFrames
    base_dir = os.path.dirname(os.path.abspath(__file__)) #Extrai como base o diretório onde o script é executado
    arquivos = os.path.join(base_dir, 'Arquivos', 'Fotovoltaica') #Busca todos os arquivos da pasta 'Fotovoltaica'
    for arquivo in os.listdir(arquivos): #Varre todos os arquivos da pasta
        if arquivo.endswith('.csv'):
            caminho_arquivo = os.path.join(arquivos, arquivo)
            df = pd.read_csv(caminho_arquivo, sep=';', dtype={13: str, 14: str}, low_memory=False) #Arquivos da ons estão separados por ;
            dfs.append(df) #Armazena todos os arquivos .cvs na lista dfs
    df = pd.concat(dfs, ignore_index=True) #Concatena todos os DataFrames em um único
    
    # Ajuste para "geração referência" maior que "disponibilidade"
    df.loc[df['val_geracaoreferencia'] > df['val_disponibilidade'], 'val_geracaoreferencia'] = df['val_disponibilidade']
    
    # Ajuste de "geração" se for maior que "disponibilidade"
    df.loc[df['val_geracao'] > df['val_disponibilidade'], 'val_geracao'] = df['val_disponibilidade']
    
    # Ajuste para geração maior que zero com a Usina desligada
    df.loc[df['val_geracaolimitada'] == 0, 'val_geracao'] = df.apply(
        lambda row: 0 if row['val_geracao'] == 0 else (
            row['val_geracao'] if row['val_geracao'] > row['val_geracaoreferencia'] and row['val_geracao'] < row['val_disponibilidade']
            else (
                row['val_geracaoreferencia'] if row['val_geracao'] > row['val_disponibilidade'] else row['val_geracao']
            )
        ), axis=1
    )
    df.loc[df['val_geracaolimitada'] == 0, 'val_geracaoreferencia'] = df.apply(
        lambda row: row['val_geracao'] if row['val_geracao'] > row['val_geracaoreferencia'] and row['val_geracao'] < row['val_disponibilidade'] else (
            row['val_disponibilidade'] if row['val_geracao'] > row['val_disponibilidade'] else row['val_geracaoreferencia']
        ), axis=1
    )

    # Preenchimento da Geração Frustrada
    df['geracao_frustrada'] = np.where(
        df['val_geracaolimitada'] != 0 & pd.notna(df['val_geracaolimitada']),  # Se geração limitada não é zero
        np.where(
            df['val_geracaoreferencia'] > df['val_geracao'],  # Se val_geracaoreferencia > val_geracao
            df['val_geracaoreferencia'] - df['val_geracao'],  # Então geracao_frustada = val_geracaoreferencia - val_geracao
            0  # Caso contrário, geracao_frustada = 0
        ),
        np.where(
            df['val_geracaolimitada'] == 0,  # Se geração limitada é 0, ONS desligou a Usina
            df['val_geracaoreferencia'],  # Então geracao_frustada = val_geracaoreferencia
            np.nan  # Caso contrário, geracao_frustada = NaN
        )
    )
    
    
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
        (df['geracao_frustrada'] <= df['val_disponibilidade']),  # Verificação adicional
        100 * (df['geracao_frustrada'] / df['val_disponibilidade']),  # Corte %
        np.nan  # Caso contrário, atribui NaN
    )
    df['Geracao MWh'] = df['val_geracao']/2 #Geração em MWh é a geração média (dada em intervalos de 30 min) / 2
    df['Geracao frustrada MWh'] = df['geracao_frustrada']/2
    df.to_parquet('dataframeFV.parquet', index=False)    
   
    return df

def read_csvEOL():
    dfs = [] #Lista base contendo todos os DataFrames
    base_dir = os.path.dirname(os.path.abspath(__file__)) #Extrai como base o diretório onde o script é executado
    arquivos = os.path.join(base_dir, 'Arquivos', 'Eólica') #Busca todos os arquivos da pasta 'Fotovoltaica'
    for arquivo in os.listdir(arquivos): #Varre todos os arquivos da pasta
        if arquivo.endswith('.csv'):
            caminho_arquivo = os.path.join(arquivos, arquivo)
            df = pd.read_csv(caminho_arquivo, sep=';', dtype={13: str, 14: str}, low_memory=False) #Arquivos da ons estão separados por ;
            dfs.append(df) #Armazena todos os arquivos .cvs na lista dfs
    df = pd.concat(dfs, ignore_index=True) #Concatena todos os DataFrames em um único
    
    # Ajuste para "geração referência" maior que "disponibilidade"
    df.loc[df['val_geracaoreferencia'] > df['val_disponibilidade'], 'val_geracaoreferencia'] = df['val_disponibilidade']
    
    # Ajuste de "geração" se for maior que "disponibilidade"
    df.loc[df['val_geracao'] > df['val_disponibilidade'], 'val_geracao'] = df['val_disponibilidade']
    
    # Ajuste para geração maior que zero com a Usina desligada
    df.loc[df['val_geracaolimitada'] == 0, 'val_geracao'] = df.apply(
        lambda row: 0 if row['val_geracao'] == 0 else (
            row['val_geracao'] if row['val_geracao'] > row['val_geracaoreferencia'] and row['val_geracao'] < row['val_disponibilidade']
            else (
                row['val_geracaoreferencia'] if row['val_geracao'] > row['val_disponibilidade'] else row['val_geracao']
            )
        ), axis=1
    )
    df.loc[df['val_geracaolimitada'] == 0, 'val_geracaoreferencia'] = df.apply(
        lambda row: row['val_geracao'] if row['val_geracao'] > row['val_geracaoreferencia'] and row['val_geracao'] < row['val_disponibilidade'] else (
            row['val_disponibilidade'] if row['val_geracao'] > row['val_disponibilidade'] else row['val_geracaoreferencia']
        ), axis=1
    )

    # Preenchimento da Geração Frustrada
    df['geracao_frustrada'] = np.where(
        df['val_geracaolimitada'] != 0 & pd.notna(df['val_geracaolimitada']),  # Se geração limitada não é zero
        np.where(
            df['val_geracaoreferencia'] > df['val_geracao'],  # Se val_geracaoreferencia > val_geracao
            df['val_geracaoreferencia'] - df['val_geracao'],  # Então geracao_frustada = val_geracaoreferencia - val_geracao
            0  # Caso contrário, geracao_frustada = 0
        ),
        np.where(
            df['val_geracaolimitada'] == 0,  # Se geração limitada é 0, ONS desligou a Usina
            df['val_geracaoreferencia'],  # Então geracao_frustada = val_geracaoreferencia
            np.nan  # Caso contrário, geracao_frustada = NaN
        )
    )
    
    
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
        (df['geracao_frustrada'] <= df['val_disponibilidade']),  # Verificação adicional
        100 * (df['geracao_frustrada'] / df['val_disponibilidade']),  # Corte %
        np.nan  # Caso contrário, atribui NaN
    )
    df['Geracao MWh'] = df['val_geracao']/2 #Geração em MWh é a geração média (dada em intervalos de 30 min) / 2
    df['Geracao frustrada MWh'] = df['geracao_frustrada']/2
    df = df[df['val_disponibilidade'] >= 0]
    df.to_parquet('dataframeEOL.parquet', index=False)
    return df

script_dir = os.path.dirname(os.path.abspath(__file__))
file_pathFV = os.path.join(script_dir, 'dataframeFV.parquet')
file_pathEOL = os.path.join(script_dir, 'dataframeEOL.parquet')

# Verifica se o arquivo existe no mesmo diretório do script e que nenhum arquivo novo foi baixado
if os.path.exists(file_pathFV) and not manager.downloadrealizadoFV:
    # Carrega o DataFrame se o arquivo já existir
    df = pd.read_parquet(file_pathFV)
    dfcoordsUFV = pd.read_csv('usinascoordsUFV.csv')
    df = df.merge(dfcoordsUFV, on='nom_usina', how='left')

    # Garantir que as colunas de latitude e longitude estejam no formato numérico (float)
    df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
    df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')
    print("DataFrame carregado a partir do arquivo existente.")
    
else:
    print("Arquivos base foram atualizados, carregando novos dados...")
    df = read_csv()
    dfcoordsUFV = pd.read_csv('usinascoordsUFV.csv')
    df = df.merge(dfcoordsUFV, on='nom_usina', how='left')

    # Garantir que as colunas de latitude e longitude estejam no formato numérico (float)
    df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
    df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')
    
if os.path.exists(file_pathEOL) and not manager.downloadrealizadoEOL:
    # Carrega o DataFrame se o arquivo já existir
    dfEOL = pd.read_parquet(file_pathEOL)
   
    dfcoordsEOL = pd.read_csv('usinascoords.csv')
    dfEOL = dfEOL.merge(dfcoordsEOL, on='nom_usina', how='left')

    # Garantir que as colunas de latitude e longitude estejam no formato numérico (float)
    dfEOL['Latitude'] = pd.to_numeric(dfEOL['Latitude'], errors='coerce')
    dfEOL['Longitude'] = pd.to_numeric(dfEOL['Longitude'], errors='coerce')
    print("DataFrame carregado a partir do arquivo existente.")
else:
    print("Arquivos base foram atualizados, carregando novos dados...")
    dfEOL = read_csvEOL()
    dfcoordsEOL = pd.read_csv('usinascoords.csv')
    dfEOL = dfEOL.merge(dfcoordsEOL, on='nom_usina', how='left')

    # Garantir que as colunas de latitude e longitude estejam no formato numérico (float)
    dfEOL['Latitude'] = pd.to_numeric(dfEOL['Latitude'], errors='coerce')
    dfEOL['Longitude'] = pd.to_numeric(dfEOL['Longitude'], errors='coerce')
 

