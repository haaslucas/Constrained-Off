import pandas as pd
import geopandas as gpd
import numpy as np

#LINHAS = gpd.read_file('Linhas2.geojson')
PASTA = '' #'constrained-off/'

UFV = gpd.read_file(PASTA + 'UFV.geojson')
UFV['conjunto'] = UFV['conjunto'].fillna('none')
UFV.nome = UFV.nome.str.lower()
UFV.conjunto = UFV.conjunto.str.lower()

CON_UFV = pd.read_parquet(PASTA + 'dataframeFV.parquet')
CON_UFV['Corte %'].fillna(0, inplace=True)

CON_UFV.nom_usina = CON_UFV.nom_usina.str.lower()
CON_UFV['conjunto'] = CON_UFV.nom_usina
CON_UFV.drop(columns=['nom_usina'], inplace=True)
CON_UFV.conjunto = CON_UFV.conjunto.str.replace('fotovoltaico ', '', regex=False)

nomes = {
'conj. fotov. acu iii 230kv': 'conj. açú iii 230 kv',
'conj. três marias 3 138kv':'conj. três marias 3 138 kv',
'conjunto santa luzia ii 500kv':'conjunto santa luzia ii 500 kv',
}
CON_UFV.conjunto.replace(nomes, inplace=True)


# criando o mapeamento entre conjuntos e subestacao
UFV = UFV.drop_duplicates(subset=['conjunto'], keep='first')
mapa_subestacao_fv = UFV.set_index('conjunto')['subestacao']
CON_UFV['subestacao'] = CON_UFV['conjunto'].map(mapa_subestacao_fv)


EOL = gpd.read_file(PASTA + 'EOL.geojson')
EOL['conjunto'] = EOL['conjunto'].fillna('none')

EOL.conjunto = EOL.conjunto.str.lower()

CON_EOL = pd.read_parquet(PASTA + 'dataframeEOL.parquet')
CON_EOL['Corte %'].fillna(0, inplace=True)

CON_EOL.nom_usina = CON_EOL.nom_usina.str.lower()
CON_EOL['conjunto'] = CON_EOL.nom_usina
CON_EOL.drop(columns=['nom_usina'], inplace=True)
#CON_EOL.conjunto = CON_EOL.conjunto.str.replace('eólico ', '', regex=False)

EOL = EOL.drop_duplicates(subset=['conjunto'], keep='first')
mapa_subestacao_eol = EOL.set_index('conjunto')['subestacao']
CON_EOL['subestacao'] = CON_EOL['conjunto'].map(mapa_subestacao_eol)

porcentagens_por_conjunto_fv = CON_UFV['Corte %'].groupby(CON_UFV['conjunto']).mean().sort_values(ascending=False)
porcentagens_por_conjunto_fv = pd.DataFrame(porcentagens_por_conjunto_fv)
porcentagens_por_conjunto_fv['conjunto'] = porcentagens_por_conjunto_fv.index
porcentagens_por_conjunto_fv['subestacao'] = porcentagens_por_conjunto_fv['conjunto'].index.map(mapa_subestacao_fv)

porcentagens_por_conjunto_eol = CON_EOL['Corte %'].groupby(CON_EOL['conjunto']).mean().sort_values(ascending=False)
porcentagens_por_conjunto_eol = pd.DataFrame(porcentagens_por_conjunto_eol)
porcentagens_por_conjunto_eol['conjunto'] = porcentagens_por_conjunto_eol.index
porcentagens_por_conjunto_eol['subestacao'] = porcentagens_por_conjunto_eol['conjunto'].index.map(mapa_subestacao_eol)
#create a dictionary of the substations 

dicionario = {}

subestacoes_fv = CON_UFV.subestacao.unique()
subestacoes_eol = CON_EOL.subestacao.unique()
subestacoes = set(subestacoes_fv) | set(subestacoes_eol)

CON_UFV.to_parquet(PASTA + 'constrained-off_fv.parquet', engine='pyarrow', compression='gzip')
CON_EOL.to_parquet(PASTA + 'constrained-off_eol.parquet', engine='pyarrow', compression='gzip')

######################################
# daqui pra baixo não é utilizado mais
for sub in subestacoes:
    cortes_fv_dessa_subestacao = CON_UFV[CON_UFV['subestacao'] == sub]
    cortes_eol_dessa_subestacao = CON_EOL[CON_EOL['subestacao'] == sub]
    cortes_dessa_subestacao = pd.concat([cortes_fv_dessa_subestacao, cortes_eol_dessa_subestacao], ignore_index=True)
    
    sub_nome = 'SE ' + str(sub)
    conjuntos_dessa_subestacao = cortes_dessa_subestacao['conjunto'].unique()
    dicionario[sub_nome] = {
        'corte_subestacao': cortes_dessa_subestacao['Corte %'].mean(),
        'corte_por_conjunto_fv': cortes_fv_dessa_subestacao.groupby('conjunto')['Corte %'].mean().sort_values(ascending=False).to_dict(),
        'corte_por_conjunto_eol': cortes_eol_dessa_subestacao.groupby('conjunto')['Corte %'].mean().sort_values(ascending=False).to_dict(),
        }
        

import json
with open(PASTA + 'cortes_por_subestacao.json', 'w', encoding='utf-8') as f:
    json.dump(dicionario, f, ensure_ascii=False, indent=4)
    
    
for conjunto_fv in CON_UFV.conjunto.unique():
    a=1
conjuntos_eol = {
    'conj_eolico_1': 'porcentagem_corte',
    'conj_eolico_2': 'porcentagem_corte',
    'conj_eolico_3': 'porcentagem_corte',
}

conjuntos_fv = {
    'conj_fotovoltaico_1': 'porcentagem_corte',
    'conj_fotovoltaico_2': 'porcentagem_corte',
    'conj_fotovoltaico_3': 'porcentagem_corte',
}

dicionario = {
    'subestacao': None,
    'corte_medio': None,
    'conjuntos_eol': conjuntos_eol,    
    'conjuntos_fv': conjuntos_fv,
}