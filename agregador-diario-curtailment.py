import pandas as pd
import numpy as np

PASTA = '' #

# 1) Carrega os dados
CON_EOL = pd.read_parquet('constrained-off_eol.parquet', engine='pyarrow')
CON_UFV = pd.read_parquet('constrained-off_fv.parquet', engine='pyarrow')

CON_EOL.index = CON_EOL.din_instante
CON_UFV.index = CON_UFV.din_instante
CON_EOL.drop(columns=['din_instante'], inplace=True)
CON_UFV.drop(columns=['din_instante'], inplace=True)
# 2) agrupa por conjunto + dia e soma val_geracao

CON_EOL.fillna({'cod_razaorestricao': 'None'}, inplace=True)
CON_UFV.fillna({'cod_razaorestricao': 'None'}, inplace=True)

CON_EOL.rename(columns={
    'val_geracao': 'desp',
    'val_geracaolimitada': 'val_geracaolimitada',
    'val_disponibilidade': 'disp',
    'val_geracaoreferencia': 'ref',
    'geracao_frustrada': 'geracao_frustrada',
    'Geracao MWh': 'Geracao_MWh',
    'Geracao frustrada MWh': 'Geracao_frustrada_MWh'
}, inplace=True)

CON_UFV.rename(columns={
    'val_geracao': 'desp',
    'val_geracaolimitada': 'val_geracaolimitada',
    'val_disponibilidade': 'disp',
    'val_geracaoreferencia': 'ref',
    'geracao_frustrada': 'geracao_frustrada',
    'Geracao MWh': 'Geracao_MWh',
    'Geracao frustrada MWh': 'Geracao_frustrada_MWh'
}, inplace=True)

CON_EOL['denominador'] = [CON_EOL['desp'] if CON_EOL['desp'] > CON_EOL['ref'] else CON_EOL['ref']]
CON_UFV['denominador'] = [CON_UFV['desp'] if CON_UFV['desp'] > CON_UFV['ref'] else CON_UFV['ref']]

CON_EOL['curtailment'] [CON_EOL['ref'] - CON_EOL['desp'] if CON_EOL['desp'] < CON_EOL['ref'] and CON_EOL['disp'] > CON_EOL['ref'] else 0]
CON_UFV['curtailment'] = [CON_UFV['ref'] - CON_UFV['desp'] if CON_UFV['desp'] < CON_UFV['ref'] and CON_UFV['disp'] > CON_UFV['ref'] else 0]

daily_mean_eol = CON_EOL.groupby(['conjunto', pd.Grouper(freq='D')])['val_geracao'].mean().reset_index()
daily_mean_ufv = CON_UFV.groupby(['conjunto', pd.Grouper(freq='D')])['val_geracao'].mean().reset_index()

# 2) Separa as colunas numéricas das não-numéricas. 
# A separação pode ser feita tanto para EOL quanto para UFV,
# as colunas são mesmas.

num_cols = CON_EOL.select_dtypes(include='number').columns
obj_cols = CON_EOL.select_dtypes(exclude='number').columns
obj_cols = [c for c in obj_cols if c != 'conjunto']


# 3) Monta o dicionário de agregação
agg_dict = {c: 'mean'   for c in num_cols} # mean for numerical columns
agg_dict.update({c: 'first' for c in obj_cols}) # first for non-numerical columns


dailly_all_eol = CON_EOL.groupby(['conjunto', pd.Grouper(freq='D')]).agg(agg_dict).reset_index()
dailly_all_ufv = CON_UFV.groupby(['conjunto', pd.Grouper(freq='D')]).agg(agg_dict).reset_index()

dailly_all_eol['tipo'] = 'eol'
dailly_all_ufv['tipo'] = 'ufv'

dailly_all = pd.concat([dailly_all_eol, dailly_all_ufv], ignore_index=True)
#df_diario2 = (CON_EOL.groupby(['conjunto',pd.Grouper(freq='D')])['val_geracao'].sum())


############################################

'''
tentativa de agrupar os dados não numéricos por moda e não por primeira ocorrência.
Fica muito lentoe portanto foi abandonada.

# separa as colunas numéricas e não-numéricas
num_cols = CON_EOL.select_dtypes(include='number').columns
obj_cols = [c for c in CON_EOL.select_dtypes(exclude='number').columns
            if c != 'conjunto']          # chave do groupby

# --- 1) função que devolve a moda (primeira em caso de empate) ---
def moda(s):
    # value_counts é mais rápido que mode() e já lida com NaN se dropna=True
    vc = s.value_counts(dropna=True)
    if vc.empty:          # coluna toda vazia
        return np.nan
    return vc.idxmax()    # primeira ocorrência do maior contador

# --- 2) dicionário de agregação ---
agg_dict = {c: 'mean' for c in num_cols}      # média nos numéricos
agg_dict.update({c: moda for c in obj_cols})  # moda nos não-numéricos

dailly_all_eol2 = CON_EOL.groupby(['conjunto', pd.Grouper(freq='D')]).agg(agg_dict).reset_index()
dailly_all_ufv2 = CON_UFV.groupby(['conjunto', pd.Grouper(freq='D')]).agg(agg_dict).reset_index()

dailly_all2 = pd.concat([dailly_all_eol2, dailly_all_ufv2], ignore_index=True)'''

dailly_all.to_parquet('constrained_agregado_diario.parquet')
