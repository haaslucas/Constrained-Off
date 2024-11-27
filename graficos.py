import dados
#import folium

df = dados.df
dfEOL = dados.dfEOL
#Análise regional de todos os dados

def media_por_estado(df):
    return df.groupby(['nom_estado', 'Mes'])['Geracao frustrada MWh'].mean().reset_index()

def corte_por_estado(df):
    return df.groupby(['nom_estado', 'Mes', 'cod_razaorestricao'])['Corte %'].mean().reset_index()
    

def total_por_estado(df):
    return df.groupby(['nom_estado', 'Mes', 'cod_razaorestricao', 'nom_usina'])['Geracao frustrada MWh'].sum().reset_index()

def media_por_estado_hora(df):
    return df.groupby(['nom_estado', 'Hora'])['geracao_frustrada'].mean().reset_index()

def media_por_restricao(df):
    return df.groupby('cod_razaorestricao')['geracao_frustrada'].mean().reset_index()

# Funções que calculam percentuais
def percentuais_por_tipo(df):
    tipo_estado = df[df['val_geracaolimitada'].notna() & (df['geracao_frustrada'] != 0)].groupby('cod_razaorestricao')['geracao_frustrada'].sum()
    tipo_total = tipo_estado.sum()
    percentuais = (tipo_estado / tipo_total) * 100
    return percentuais.reset_index(name='geracao_frustrada')  # Renomeando a coluna

def percentuais_por_estado(df):
    estado = df[df['geracao_frustrada'].notna() & (df['geracao_frustrada'] != 0)].groupby('nom_estado')['geracao_frustrada'].count()
    estado_total = estado.sum()    
    # Calculando os percentuais e retornando como DataFrame
    percentuais = (estado / estado_total) * 100
    return percentuais.reset_index(name='geracao_frustrada')  # Renomeando a coluna

def percentuais_por_regiao(df):
    regiao = df[df['geracao_frustrada'].notna() & (df['geracao_frustrada'] != 0)].groupby('id_subsistema')['geracao_frustrada'].count()
    regiao_total = regiao.sum()
    percentuais = (regiao / regiao_total) * 100
    return percentuais.reset_index(name='geracao_frustrada')  # Renomeando a coluna

def percentuais_por_hora(df):
    hora = df[df['geracao_frustrada'].notna() & (df['geracao_frustrada'] != 0)].groupby('Hora')['geracao_frustrada'].count()
    hora_total = hora.sum()
    return ((hora / hora_total) * 100).map(lambda x: f'{x:.2f}%').reset_index()

''' FUNCIONA BEM O PRIMEIRO
#Gráficos com mapa do Brasil

#Magnitude por estado
estados_data = {
    'estado': ['ACRE', 'ALAGOAS', 'AMAPÁ', 'AMAZONAS', 'BAHIA', 'CEARÁ', 'DISTRITO FEDERAL', 'ESPÍRITO SANTO',
               'GOIÁS', 'MARANHÃO', 'MATO GROSSO', 'MATO GROSSO DO SUL', 'MINAS GERAIS', 'PARÁ', 'PARAÍBA', 'PARANÁ',
               'PERNAMBUCO', 'PIAUÍ', 'RIO DE JANEIRO', 'RIO GRANDE DO NORTE', 'RIO GRANDE DO SUL', 'RONDÔNIA',
               'RORAIMA', 'SANTA CATARINA', 'SÃO PAULO', 'SERGIPE', 'TOCANTINS'],
    'lat': [-9.974, -9.5713, 0.902, -3.4168, -12.9704, -3.7172, -15.7801, -19.1834, -15.827, -2.5307, -12.6819,
            -20.4428, -18.5122, -1.455, -7.1202, -25.2521, -8.0476, -5.092, -22.9068, -5.7945, -30.0346, -11.5057,
            2.8235, -27.5954, -23.5505, -10.9472, -10.1753],
    'lon': [-67.824, -36.9054, -52.0469, -65.8561, -38.5124, -38.5434, -47.9292, -40.3089, -49.8362, -44.298,
            -56.925, -54.6469, -44.555, -48.502, -34.8707, -52.0215, -34.877, -42.8019, -43.1729, -35.2094, -51.2177,
            -63.5806, -60.6758, -48.548, -46.6333, -37.0731, -48.2982]
}

# Converte para DataFrame
estados_df = pd.DataFrame(estados_data)

# Calcula o somatório da geração limitada por estado
gerlmtd = df.groupby('nom_estado')['val_geracaolimitada'].sum().reset_index()

# Renomeia a coluna para 'magnitude' para facilitar a mesclagem
gerlmtd.rename(columns={'nom_estado': 'estado', 'val_geracaolimitada': 'magnitude'}, inplace=True)

# Mescla os dados de magnitudes com os estados, preenchendo com 0 para estados não encontrados
estados_df = estados_df.merge(gerlmtd, on='estado', how='left').fillna(0)

# Cria o mapa centrado no Brasil
mapa_brasil = folium.Map(location=[-14.2350, -51.9253], zoom_start=4)

# Adiciona círculos ao mapa apenas se a magnitude for maior que 0
for index, row in estados_df.iterrows():
    latitude = row['lat']
    longitude = row['lon']
    magnitude = row['magnitude']

    if magnitude > 0:  # Apenas adiciona se a magnitude for maior que 0
        # Formata a magnitude com vírgula como separador decimal
        magnitude_formatada = str(magnitude).replace('.', ',') + ' MVA'
        
        folium.CircleMarker(
            location=(latitude, longitude),
            radius=magnitude / 50000,  # Ajuste o raio conforme necessário
            color='blue',
            fill=True,
            fill_color='blue',
            fill_opacity=0.6,
            popup=f'{row["estado"]}: {magnitude_formatada}'
        ).add_to(mapa_brasil)

# Exibe o mapa
mapa_brasil.save("mapa_cortes.html")
'''
'''
#Separando por cores distintas razões de restrição

cor_restricao = {
    'REL': 'red',  # INDISPONIBILIDADE EXTERNA
    'CNF': 'orange',  # CONFIABILIDADE
    'ENE': 'green',  # RAZÃO ENERGÉTICA
    'PAR': 'blue'    # PARECER DE ACESSO
}

# Mapeia a razão de restrição para as cores
df['cor'] = df['cod_razaorestricao'].map(cor_restricao)

# Cria o mapa centrado no Brasil
mapa_brasil = folium.Map(location=[-14.2350, -51.9253], zoom_start=4)

# Adiciona círculos ao mapa apenas se a magnitude for maior que 0
for index, row in estados_df.iterrows():
    latitude = row['lat']
    longitude = row['lon']
    magnitude = row['magnitude']
    
    # Verifica se há registros na base de dados para a razão de restrição
    restricao = df.loc[df['nom_estado'] == row['estado'], 'cod_razaorestricao']
    restricao = restricao.dropna()
    # Ignora estados sem restrição
    if restricao.empty:
        continue  # Ignora se não houver restrição
    
     # Itera sobre todas as restrições do estado
    for restricao_codigo in restricao:
    
        # Verifica se o código de restrição está no dicionário de cores
        if restricao_codigo in cor_restricao:
            cor = cor_restricao[restricao_codigo]
        else:
            print(f"Código de restrição {restricao_codigo} não encontrado nas cores.")
            continue  # Ignora se o código não estiver no dicionário de cores
    
        if magnitude > 0:  # Apenas adiciona se a magnitude for maior que 0
            # Formata a magnitude com vírgula como separador decimal
            magnitude_formatada = str(magnitude).replace('.', ',') + ' MVA'
            
            folium.CircleMarker(
                location=(latitude, longitude),
                radius=magnitude / 50000,  # Ajuste o raio conforme necessário
                color=cor,
                fill=True,
                fill_color=cor,
                fill_opacity=0.6,
                popup=f'{row["estado"]}: {magnitude_formatada} (Restrição: {restricao_codigo})'
            ).add_to(mapa_brasil)

# Adiciona uma legenda ao mapa
legend_html = """
    <div style="position: fixed; 
                bottom: 50px; left: 50px; width: 150px; height: 100px; 
                background-color: white; opacity: .8; z-index:9999; 
                font-size:14px; padding: 10px;">
        <b>Legenda</b><br>
        <i style="color:red;">●</i> INDISPONIBILIDADE EXTERNA (REL)<br>
        <i style="color:orange;">●</i> CONFIABILIDADE (CNF)<br>
        <i style="color:green;">●</i> RAZÃO ENERGÉTICA (ENE)<br>
        <i style="color:blue;">●</i> PARECER DE ACESSO (PAR)<br>
        <i style="color:gray;">●</i> Sem restrição
    </div>
"""
mapa_brasil.get_root().html.add_child(folium.Element(legend_html))

# Exibe o mapa
mapa_brasil.save("mapa_cortes_restricoes.html")
'''
