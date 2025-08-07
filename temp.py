import pandas as pd
import geopandas as gpd
import numpy as np

LINHAS = gpd.read_file('Linhas2.geojson')

EOL = gpd.read_file('EOL.geojson')
UFV = gpd.read_file('UFV.geojson')

CON_EOL = pd.read_parquet('dataframeEOL2.parquet')
CON_UFV = pd.read_parquet('dataframeFV2.parquet')

#set all to low caps
CON_UFV.nom_usina = CON_UFV.nom_usina.str.lower()
UFV.nome = UFV.nome.str.lower()

UFV.nome.str.contains('luzia')
CON_UFV.nom_usina.str.contains('luzia')
a=1