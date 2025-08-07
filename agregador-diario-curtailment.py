import pandas as pd
import numpy as np

PASTA = '' #'constrained-off/'

# 1) Carrega os dados
CON_EOL = pd.read_parquet('constrained-off_eol.parquet', engine='pyarrow')
CON_UFV = pd.read_parquet('constrained-off_fv.parquet', engine='pyarrow')

# 2) agrupa por conjunto + dia e soma val_geracao

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

dailly_all2 = pd.concat([dailly_all_eol2, dailly_all_ufv2], ignore_index=True)

dailly_all.to_parquet('constrained_ufv_diario_media.parquet', index=False)