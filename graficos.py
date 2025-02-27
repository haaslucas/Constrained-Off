import dados
import folium
import imgkit

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

# Funções que calculam percentuais
def total_por_tipo(df):
    total_tipo = df[df['val_geracaolimitada'].notna() & (df['geracao_frustrada'] != 0)].groupby('cod_razaorestricao')['geracao_frustrada'].sum()
    return total_tipo

def total_por_tipo_normalizado(df):
    # Filtra o DataFrame para os registros desejados
    filtro = df['val_geracaolimitada'].notna() & (df['geracao_frustrada'] != 0)
    df_filtrado = df[filtro]

    # Agrupa por 'cod_razaorestricao' e soma os valores normalizados
    total_tipo_normalizado = df_filtrado.groupby('cod_razaorestricao')['geracao_frustrada'].sum()/df_filtrado['geracao_frustrada'].sum()
    return total_tipo_normalizado


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

# Função para calcular o total de Geração Frustrada MWh
def calcular_geracao(df_filtrado, data_inicio, data_fim):
    df_filtrado = df_filtrado[(df_filtrado['Dia'] >= data_inicio) & (df_filtrado['Dia'] <= data_fim)]
    soma_geracao = df_filtrado['Geracao frustrada MWh'].sum()
    return soma_geracao


import matplotlib.pyplot as plt
from io import BytesIO
import base64
import os


def gerar_mapa(df, estado, tipo_restricao, usina):
    # Filtrar os dados com base nos filtros
    if estado != "Todos":
        df = df[df['nom_estado'] == estado]
    if tipo_restricao != "Todos":
        df = df[df['cod_razaorestricao'] == tipo_restricao]
    if usina != "Todos":
        df = df[df['nom_usina'] == usina]

    # Verificar se há dados após os filtros
    if df.empty:
        print("Nenhum dado disponível após aplicar os filtros.")
        mapa = folium.Map(location=[-15.0, -55.0], zoom_start=4.2)
        mapa.save("mapa_interativo.html")
        imgkit.from_file("mapa_interativo.html", "mapa.png")
        return "mapa.png"

    # Criar o mapa base com zoom ajustado
    mapa = folium.Map(location=[-15.0, -55.0], zoom_start=4.2)

    # Cores para os tipos de restrição
    cores = {
        "CNF": "#FF6347",  # Vermelho
        "ENE": "#FFD700",  # Amarelo
        "REL": "#1E90FF",  # Azul
    }

    # Agrupar os dados por usina e tipo de corte
    usinas_agrupadas = (
        df.groupby(['nom_usina', 'Latitude', 'Longitude', 'cod_razaorestricao'])
        .agg({'geracao_frustrada': 'sum'})
        .reset_index()
    )

    # Calcular escala para o tamanho dos gráficos
    geracao_total = (
        usinas_agrupadas.groupby(['nom_usina', 'Latitude', 'Longitude'])
        .agg({'geracao_frustrada': 'sum'})
        .reset_index()
    )

    min_geracao = geracao_total['geracao_frustrada'].min()
    max_geracao = geracao_total['geracao_frustrada'].max()

    def calcular_tamanho(geracao):
        """Função para calcular tamanho baseado na geração frustrada."""
        tamanho_min = 0.6  # Raio mínimo
        tamanho_max = 1.5  # Raio máximo
        if max_geracao == min_geracao:
            return tamanho_min  # Caso extremo: todas as usinas têm a mesma geração
        return tamanho_min + (tamanho_max - tamanho_min) * ((geracao - min_geracao) / (max_geracao - min_geracao))

    for usina, group in usinas_agrupadas.groupby(['nom_usina', 'Latitude', 'Longitude']): 
        nome_usina, lat, lon = usina

        # Dados para o gráfico de setores
        geracao_por_corte = group.set_index('cod_razaorestricao')['geracao_frustrada']
        geracao_total_usina = geracao_por_corte.sum()

        # Ignorar usinas sem dados ou com geração frustrada igual a zero
        if geracao_total_usina == 0 or geracao_por_corte.isnull().any():
            continue

        # Calcular o tamanho do gráfico
        tamanho_base = calcular_tamanho(geracao_total_usina)

        # Criar o gráfico de setores
        fig, ax = plt.subplots(figsize=(tamanho_base, tamanho_base), dpi=100)
        ax.pie(
            geracao_por_corte,
            labels=None,  # Remove rótulos do gráfico
            colors=[cores.get(tipo, "gray") for tipo in geracao_por_corte.index],
            autopct=lambda pct: f"{pct:.1f}%" if pct > 5 else "",  # Percentuais >5%
        )
        plt.axis('equal')

        # Salvar o gráfico como imagem base64
        img_buffer = BytesIO()
        plt.savefig(img_buffer, format='png', bbox_inches='tight', transparent=True)
        plt.close(fig)
        img_buffer.seek(0)
        img_base64 = base64.b64encode(img_buffer.read()).decode()

        # Adicionar o gráfico ao mapa como um DivIcon escalável
        folium.Marker(
            location=[lat, lon],
            icon=folium.DivIcon(
                html=f"""
                <div style="
                    transform: translate(-50%, -50%) scale(0.8);
                    position: fixed; 
                    width: {tamanho_base * 50}px; 
                    height: {tamanho_base * 50}px;">
                    <img src="data:image/png;base64,{img_base64}" 
                         style="width: 100%; height: 100%; border-radius: 50%;">
                </div>
                """
            ),
            popup=(f"<b>Usina:</b> {nome_usina}<br>"
                   f"<b>Total Geração Frustrada:</b> {geracao_total_usina:.2f}"),
        ).add_to(mapa)

    # Adicionar legenda
    legend_html = """
    <div style="
        position: fixed;
        bottom: 50px;
        left: 50px;
        width: 250px;
        height: 140px;
        background-color: white;
        border:2px solid grey;
        z-index:9999;
        font-size:14px;
        padding: 10px;
    ">
        <b>Legenda:</b><br>
        <i style="background: #FF6347; width: 10px; height: 10px; display: inline-block;"></i> CNF<br>
        <i style="background: #FFD700; width: 10px; height: 10px; display: inline-block;"></i> ENE<br>
        <i style="background: #1E90FF; width: 10px; height: 10px; display: inline-block;"></i> REL<br>
    </div>
    """
    mapa.get_root().html.add_child(folium.Element(legend_html))

    # Adicionar o botão para voltar à página de filtros
    button_html = """
    <div style="
        position: fixed;
        bottom: 10px;
        right: 10px;
        z-index: 9999;
        background-color: #007bff;
        color: white;
        padding: 10px 20px;
        border-radius: 5px;
        cursor: pointer;
    " onclick="window.location.href='http://127.0.0.1:8001/filtros';">
        Voltar para Filtros
    </div>
    """

    mapa.get_root().html.add_child(folium.Element(button_html))

    # Salvar o mapa interativo
    dir_mapas = os.path.join(os.getcwd(), 'www')
    if not os.path.exists(dir_mapas):
        os.makedirs(dir_mapas)
    
    mapa_html = os.path.join(dir_mapas, 'mapa_interativo.html')
    
    # Excluir o arquivo antigo, se existir
    if os.path.exists(mapa_html):
        os.remove(mapa_html)
        
    mapa.save(mapa_html)

    return mapa_html



