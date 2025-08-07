import pandas as pd
import numpy as np
import geopandas as gpd

df = pd.read_parquet('dailly_curtailment.parquet', engine='pyarrow')

UFV = gpd.read_file('UFV.geojson')
subs = 
a=1