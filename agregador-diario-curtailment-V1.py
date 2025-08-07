import pandas as pd
import numpy as np

PASTA = '' #'constrained-off/'

CON_UFV = pd.read_parquet('constrained-off_fv.parquet', engine='pyarrow')



#CON_EOL = pd.read_parquet('constrained-off_eol.parquet', engine='pyarrow')

#CON_UFV.dex is datetime. Resample to daily frequency
#CON_UFV.groupby('conjunto').mean().reset_index(inplace=True)
#CON_UFV['data_hora'] = pd.to_datetime(CON_UFV.index, errors='coerce')
# 2) agrupa por conjunto + dia e soma val_geracao
#get the daily mean generation of each 'conjunto' in CON_UFV
daily_mean = CON_UFV.groupby(['conjunto', pd.Grouper(freq='D')])['val_geracao'].mean().reset_index()

# 2) Separa as colunas numéricas das não-numéricas
num_cols = CON_UFV.select_dtypes(include='number').columns
obj_cols = CON_UFV.select_dtypes(exclude='number').columns
obj_cols = [c for c in obj_cols if c != 'conjunto']


    
#now, lets do it for all columns what we done for 'val_geracao'
daily_all = CON_UFV.groupby(['conjunto', pd.Grouper(freq='D')]).agg({col: 'mean' for col in num_cols}).reset_index()

# 3) Monta o dicionário de agregação
agg_dict = {c: 'mean'   for c in num_cols}
agg_dict.update({c: 'first' for c in obj_cols})

daily_all2 = CON_UFV.groupby(['conjunto', pd.Grouper(freq='D')]).agg(agg_dict).reset_index()

# 4) Reamostra por dia aplicando cada agregação
df_diario = CON_UFV.resample('D').agg(agg_dict)


df_diario = (CON_UFV.groupby(['conjunto',pd.Grouper(freq='D')])['val_geracao'].sum())

data_inicio = '2025-01-01'
data_fim = '2025-01-10'



dicionario = {}

subestacoes_fv = CON_UFV.subestacao.unique()
subestacoes_eol = CON_EOL.subestacao.unique()
subestacoes = set(subestacoes_fv) | set(subestacoes_eol)

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

a=1