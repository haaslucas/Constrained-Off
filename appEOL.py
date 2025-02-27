from graficos import (
    total_por_estado,
    percentuais_por_tipo, percentuais_por_estado, percentuais_por_regiao, corte_por_estado, total_por_tipo_normalizado, calcular_geracao
)
import matplotlib.pyplot as plt
import numpy as np
from shiny import run_app
import pandas as pd
from shiny import App, render, reactive, ui
from dados import dfEOL
import subprocess
import webbrowser
import threading
import time
import requests

# Função para iniciar o servidor Starlette
def iniciar_servidor_starlette():
    # Construa a URL do servidor Starlette com os parâmetros de filtro
    url = "http://127.0.0.1:8001/filtros"
    # Execute o servidor Starlette em um subprocesso
    subprocess.Popen(["python", "starlette_serverEOL.py"])
    print("Aguardando o servidor iniciar...")
    for _ in range(30):  # Tenta por até 30 segundos
        try:
            # Verifica se o servidor responde
            response = requests.get(url)
            if response.status_code == 200:
                print("Servidor iniciado com sucesso!")
                break
        except requests.ConnectionError:
            pass
        time.sleep(1)  # Aguarda 1 segundo antes de tentar novamente
    else:
        print("Erro: Não foi possível conectar ao servidor.")
        return
    # Abra o URL no navegador padrão
    webbrowser.open(url)


def iniciar_servidor_shiny():
    run_app(App(app_ui, server), host="127.0.0.1", port=8000)
    webbrowser.open("http://127.0.0.1:8000")
    
    
print('Iniciando aplicativo...')

app_ui = ui.page_fluid(
    ui.h2("Constrained Off de Usinas Eólicas - Dados Abertos ONS"),

    # Navegação por abas
    ui.navset_tab(
        # Aba Dashboard
        ui.nav_panel(
            "Dashboard",
            ui.row(
                ui.column(3, ui.input_select("estado", "Selecione o Estado:", choices=["Todos"] + list(dfEOL['nom_estado'].unique()))),
                ui.column(3, ui.input_select("tipo_restricao", "Selecione a Restrição:", choices=["Todos"] + list(dfEOL['cod_razaorestricao'].dropna().unique()))),
                ui.column(3, ui.input_select("usina", "Selecione a Usina:", choices=["Todos"])),
                ui.column(3, 
                          ui.input_slider("data_slider", "Selecione o Período:", 
                                          min = min(dfEOL['Dia']), 
                                          max = max(dfEOL['Dia']), 
                                          value = [min(dfEOL['Dia']), max(dfEOL['Dia'])], 
                                          step=1)
                )
            ),
            ui.row(
                ui.column(4, ui.output_plot("por_tipo")),
                ui.column(4, ui.output_plot("por_regiao")),
                ui.column(4, ui.output_plot("por_estado"))
            ),
            ui.output_plot("total"),
            ui.output_plot("total_estado"),
            ui.output_plot("corte_estado"),
            ui.output_plot("media_diaria"),
            ui.output_text("geracao_frustrada")
        ),

        # Aba Comparativos
        ui.nav_panel(
            "Comparativos",
            ui.h3("Comparação de Usinas"),
            # Filtros organizados conforme solicitado
            ui.row(
                ui.column(4, ui.input_select("estado1", "Estado (Usina 1):", choices=["Todos"] + list(dfEOL['nom_estado'].unique()))),
                ui.column(4, ui.input_select("estado2", "Estado (Usina 2):", choices=["Todos"] + list(dfEOL['nom_estado'].unique()))),
                ui.column(4, ui.input_select("estado3", "Estado (Usina 3):", choices=["Todos"] + list(dfEOL['nom_estado'].unique())))
            ),
            ui.row(
                ui.column(4, ui.input_select("usina1", "Usina 1:", choices=["Todos"])),
                ui.column(4, ui.input_select("usina2", "Usina 2:", choices=["Todos"])),
                ui.column(4, ui.input_select("usina3", "Usina 3:", choices=["Todos"]))
            ),
            # Gráfico comparativo
            ui.output_plot("comparativo_grafico")
        )
    )
)

def server(input, output, session):
    
    
    @reactive.Effect #Atualiza o filtro de Usina de acordo com o estado selecionado
    @reactive.event(input.estado)
    def atualizar_opcoes_usina():
        # Filtra as usinas de acordo com o estado selecionado
        if input.estado() == "Todos":
            # Mostra todas as usinas caso o estado seja "Todos"
            opcoes_usina = ["Todos"] + list(dfEOL['nom_usina'].dropna().unique())
        else:
            # Filtra as usinas somente para o estado selecionado
            opcoes_usina = ["Todos"] + list(dfEOL[dfEOL['nom_estado'] == input.estado()]['nom_usina'].dropna().unique())
        
        # Atualiza as opções do filtro de usina
        ui.update_select("usina", choices=opcoes_usina)
        
 # Atualiza o filtro de usina 1 com base no estado 1
    @reactive.Effect
    @reactive.event(input.estado1)
    def atualizar_opcoes_usina1():
        if input.estado1() == "Todos":
            opcoes_usina1 = ["Todos"] + list(dfEOL['nom_usina'].dropna().unique())
        else:
            opcoes_usina1 = ["Todos"] + list(dfEOL[dfEOL['nom_estado'] == input.estado1()]['nom_usina'].dropna().unique())
        ui.update_select("usina1", choices=opcoes_usina1)

    # Atualiza o filtro de usina 2 com base no estado 2
    @reactive.Effect
    @reactive.event(input.estado2)
    def atualizar_opcoes_usina2():
        if input.estado2() == "Todos":
            opcoes_usina2 = ["Todos"] + list(dfEOL['nom_usina'].dropna().unique())
        else:
            opcoes_usina2 = ["Todos"] + list(dfEOL[dfEOL['nom_estado'] == input.estado2()]['nom_usina'].dropna().unique())
        ui.update_select("usina2", choices=opcoes_usina2)

    # Atualiza o filtro de usina 3 com base no estado 3
    @reactive.Effect
    @reactive.event(input.estado3)
    def atualizar_opcoes_usina3():
        if input.estado3() == "Todos":
            opcoes_usina3 = ["Todos"] + list(dfEOL['nom_usina'].dropna().unique())
        else:
            opcoes_usina3 = ["Todos"] + list(dfEOL[dfEOL['nom_estado'] == input.estado3()]['nom_usina'].dropna().unique())
        ui.update_select("usina3", choices=opcoes_usina3)


    @output
    @render.plot
    def total():
        df_filtrado = dfEOL #Não há filtros inicialmente
    
        # Aplica filtro por estado se não for "Todos"
        if input.estado() != "Todos":
            df_filtrado = df_filtrado[df_filtrado['nom_estado'] == input.estado()]
    
        # Aplica filtro por tipo de restrição se não for "Todos"
        if input.tipo_restricao() != "Todos":
            df_filtrado = df_filtrado[df_filtrado['cod_razaorestricao'] == input.tipo_restricao()]

        # Aplica filtro por usina se não for "Todos"
        if input.usina() != "Todos":
            df_filtrado = df_filtrado[df_filtrado['nom_usina'] == input.usina()]

        # Aplicando a função de agregação
        media = total_por_estado(df_filtrado)

        # Convertendo 'Mes' para datetime para ordenação
        media['Mes'] = pd.to_datetime(media['Mes'], format='%b %Y')
        media = media.sort_values('Mes')

        # Agrupando por 'Mes' e 'cod_razaorestricao', somando as gerações por restrição para o mês
        media_agrupada = media.groupby(['Mes', 'cod_razaorestricao'])['Geracao frustrada MWh'].sum().unstack(fill_value=0)

        # Configurando o gráfico
        fig, ax = plt.subplots(figsize=(12, 6))
        media_agrupada.plot(kind='bar', ax=ax, stacked=True, width=0.8)

        # Títulos e rótulos
        ax.set_title('Total de Geração Frustrada por Mês e Tipo de Restrição')
        ax.set_xlabel('Mês')
        ax.set_ylabel('Geração Frustrada [MWh]')
        
        # Ajustando os rótulos do eixo x para mostrar apenas mês e ano
        ax.set_xticks(range(len(media_agrupada.index)))  # Define os locais dos rótulos
        ax.set_xticklabels(media_agrupada.index.strftime('%b %Y'), rotation=45)

        # Adicionando legenda e layout
        ax.legend(title='Tipo de Restrição', bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()

        return fig
    
    @output
    @render.plot
    def total_estado():
        df_filtrado = dfEOL #Não há filtros inicialmente
    
        # Aplica filtro por estado se não for "Todos"
        if input.estado() != "Todos":
            df_filtrado = df_filtrado[df_filtrado['nom_estado'] == input.estado()]
    
        # Aplica filtro por tipo de restrição se não for "Todos"
        if input.tipo_restricao() != "Todos":
            df_filtrado = df_filtrado[df_filtrado['cod_razaorestricao'] == input.tipo_restricao()]

        # Aplica filtro por usina se não for "Todos"
        if input.usina() != "Todos":
            df_filtrado = df_filtrado[df_filtrado['nom_usina'] == input.usina()]
    
        # Calculando o total por estado e mês
        media = total_por_estado(df_filtrado)
    
        # Convertendo a coluna 'Mes' para datetime para ordenação cronológica
        media['Mes'] = pd.to_datetime(media['Mes'], format='%b %Y')  # Ajuste o formato conforme necessário
        media = media.sort_values('Mes')  # Ordenando cronologicamente
    
        # Criando a tabela pivot com 'nom_estado' como colunas, e 'Mes' como índice
        df_pivot = media.pivot_table(index='Mes', columns='nom_estado', values='Geracao frustrada MWh', aggfunc='sum')
    
        # Configuração do gráfico com matplotlib
        fig, ax = plt.subplots(figsize=(12, 7))
        df_pivot.plot(kind='bar', ax=ax, stacked=False, width=0.8)
    
        # Ajustes do gráfico
        ax.set_title('Total de Geração Frustrada por Estado e Mês - Valores Absolutos')
        ax.set_xlabel('Mês')
        ax.set_ylabel('Geração Frustrada [MWh]')
        
        # Formatando o eixo x para exibir apenas o mês e o ano
        ax.set_xticklabels(df_pivot.index.strftime('%b %Y'), rotation=45)
        
        # Ajustes finais de legenda e layout
        ax.legend(title='Estados', bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()
        
        return fig
    
    @output
    @render.plot
    def corte_estado():
        
        df_filtrado = dfEOL #Não há filtros inicialmente
    
        # Aplica filtro por estado se não for "Todos"
        if input.estado() != "Todos":
            df_filtrado = df_filtrado[df_filtrado['nom_estado'] == input.estado()]
    
        # Aplica filtro por tipo de restrição se não for "Todos"
        if input.tipo_restricao() != "Todos":
            df_filtrado = df_filtrado[df_filtrado['cod_razaorestricao'] == input.tipo_restricao()]

        # Aplica filtro por usina se não for "Todos"
        if input.usina() != "Todos":
            df_filtrado = df_filtrado[df_filtrado['nom_usina'] == input.usina()]
            
        media = corte_por_estado(df_filtrado)
        
        # Criando a tabela pivot para plotar o gráfico, com 'Mes' no eixo x e 'nom_estado' como categorias
        df_pivot = media.pivot_table(index='Mes', columns='nom_estado', values='Corte %')
        # Criando o gráfico com matplotlib
        fig, ax = plt.subplots(figsize=(10, 6))
        # Plotar o gráfico de barras, com cada estado sendo uma cor diferente
        df_pivot.plot(kind='bar', ax=ax, stacked=False)
        # Ajustes do gráfico
        ax.set_title('Média de Geração Frustrada por Estado e Mês - Valores %')
        ax.set_xlabel('Mês')
        ax.set_ylabel('Corte % (em função da disponibilidade)')       
        # Adicionar legenda e ajustar layout
        ax.legend(title='Estados', bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()

        return fig
    
    @output
    @render.plot
    def media_diaria():
        # Se o filtro for "Todos", calculamos a média para todos os estados, caso contrário, filtramos por estado
        if input.estado() == "Todos":
            df_filtrado = dfEOL
        else:
            df_filtrado = dfEOL[dfEOL['nom_estado'] == input.estado()]
            
        if input.usina() == "Todos":
        # Todas as usinas selecionadas pega a média geral
            media = df_filtrado.groupby('Hora').agg({
                'geracao_frustrada': 'mean',  
                'val_disponibilidade': 'mean',    
                'val_geracaoreferencia': 'mean',  
                'val_geracao': 'mean'
            }).reset_index()
            media['Hora'] = pd.to_datetime(media['Hora'].astype(str), format='%H:%M:%S')
            media['Hora'] = media['Hora'].dt.strftime('%H:%M')
        else:
            # Filtra pelo nome da usina selecionada
            df_filtrado = df_filtrado[df_filtrado['nom_usina'] == input.usina()]
            
            # Agrupa por 'Hora' e 'nom_usina' para a usina específica
            media = df_filtrado.groupby(['Hora', 'nom_usina']).agg({
                'geracao_frustrada': 'mean',  
                'val_disponibilidade': 'mean',    
                'val_geracaoreferencia': 'mean',  
                'val_geracao': 'mean'
            }).reset_index()          
            media['Hora'] = pd.to_datetime(media['Hora'].astype(str), format='%H:%M:%S')
            media['Hora'] = media['Hora'].dt.strftime('%H:%M')

        # Criar o gráfico
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Plotar as linhas para cada coluna desejada
        ax.plot(media['Hora'], media['geracao_frustrada'], label='Geração Frustrada', marker='o')
        ax.plot(media['Hora'], media['val_disponibilidade'], label='Disponibilidade', marker='o')
        ax.plot(media['Hora'], media['val_geracaoreferencia'], label='Geração Referência', marker='o')
        ax.plot(media['Hora'], media['val_geracao'], label='Geração Real', marker='o')
    
        # Configurações do gráfico
        ax.set_title('Avaliação Diária do Corte')
        ax.set_xlabel('Hora')
        ax.set_ylabel('Potência [MWmed]')
        ax.legend()
        plt.xticks(rotation=45)
        plt.tight_layout()


        return fig

    @output
    @render.plot
    def por_tipo():
        df_filtrado = dfEOL
        if input.estado() != "Todos":
            df_filtrado = df_filtrado[df_filtrado['nom_estado'] == input.estado()]
        if input.tipo_restricao() != "Todos":
            df_filtrado = df_filtrado[df_filtrado['cod_razaorestricao'] == input.tipo_restricao()]
        if input.usina() != "Todos":
            df_filtrado = df_filtrado[df_filtrado['nom_usina'] == input.usina()]
        
        dados_pizza = percentuais_por_tipo(df_filtrado)
        labels = dados_pizza['cod_razaorestricao']
        sizes = dados_pizza['geracao_frustrada']

        fig, ax = plt.subplots()
        ax.pie(sizes, labels=labels, autopct=lambda p: f'{p:.1f}%', startangle=90)
        ax.axis('equal')
        
        return fig

    @output
    @render.plot
    def por_regiao():
        df_filtrado = dfEOL
        if input.estado() != "Todos":
            df_filtrado = df_filtrado[df_filtrado['nom_estado'] == input.estado()]
        if input.tipo_restricao() != "Todos":
            df_filtrado = df_filtrado[df_filtrado['cod_razaorestricao'] == input.tipo_restricao()]
        if input.usina() != "Todos":
            df_filtrado = df_filtrado[df_filtrado['nom_usina'] == input.usina()]
        
        dados_pizza = percentuais_por_regiao(df_filtrado)
        labels = dados_pizza['id_subsistema']
        sizes = dados_pizza['geracao_frustrada']

        fig, ax = plt.subplots()
        ax.pie(sizes, labels=labels, autopct=lambda p: f'{p:.1f}%', startangle=90)
        ax.axis('equal')
        
        return fig

    @output
    @render.plot
    def por_estado():
        df_filtrado = dfEOL
        if input.estado() != "Todos":
            df_filtrado = df_filtrado[df_filtrado['nom_estado'] == input.estado()]
        if input.tipo_restricao() != "Todos":
            df_filtrado = df_filtrado[df_filtrado['cod_razaorestricao'] == input.tipo_restricao()]
        if input.usina() != "Todos":
            df_filtrado = df_filtrado[df_filtrado['nom_usina'] == input.usina()]
        
        dados_pizza = percentuais_por_estado(df_filtrado)
        labels = dados_pizza['nom_estado']
        sizes = dados_pizza['geracao_frustrada']
    
        # Calcula os percentuais
        total = sum(sizes)
        percentuais = [f"{label} - {size / total * 100:.1f}%" for label, size in zip(labels, sizes)]
    
        # Criando o gráfico
        fig, ax = plt.subplots(figsize=(8, 6))
        wedges, *_ = ax.pie(
            sizes, 
            labels=None,  # Remove os rótulos diretamente do gráfico
            startangle=90
        )
        ax.axis('equal')  # Mantém o gráfico como um círculo perfeito
    
        # Adicionando uma legenda externa com os percentuais
        ax.legend(
            wedges, 
            percentuais,  # Exibe estados com percentuais
            title="Estados", 
            loc="center left", 
            bbox_to_anchor=(1, 0.5),  # Posiciona a legenda fora do gráfico
            fontsize=10
        )
    
        return fig
    
    @output()
    @render.text
    def geracao_frustrada():
        
        df_filtrado = dfEOL

        if input.estado() != "Todos":
            df_filtrado = df_filtrado[df_filtrado['nom_estado'] == input.estado()]
        
        if input.tipo_restricao() != "Todos":
            df_filtrado = df_filtrado[df_filtrado['cod_razaorestricao'] == input.tipo_restricao()]
        
        if input.usina() != "Todos":
            df_filtrado = df_filtrado[df_filtrado['nom_usina'] == input.usina()]
            
        data_inicio = input.data_slider()[0]
        data_fim = input.data_slider()[1]

        # Calcular a geração frustrada MWh no período selecionado
        soma_geracao = calcular_geracao(df_filtrado, data_inicio, data_fim)
        
        # Exibir o resultado como texto
        return f"Total de Geração Frustrada MWh entre {data_inicio.strftime('%Y-%m-%d')} e {data_fim.strftime('%Y-%m-%d')} é: {soma_geracao}"
    
    @output
    @render.plot
    def comparativo_grafico():
        usinas_selecionadas = [input.usina1(), input.usina2(), input.usina3()]
    
        # Filtra o dataframe para as usinas selecionadas (filtro de estado removido)
        df_filtrado = dfEOL[dfEOL['nom_usina'].isin(usinas_selecionadas)]
    
        if not df_filtrado.empty:
            # Obter os tipos de restrição disponíveis
            restricoes = df_filtrado['cod_razaorestricao'].unique()
            restricoes = [r for r in restricoes if pd.notna(r)]  # Filtra valores NaN
    
            # Prepara os dados para o gráfico de radar
            categorias = restricoes
            usinas_dados = []
    
            for usina in usinas_selecionadas:
                # Filtra os dados para a usina atual
                df_usina = df_filtrado[df_filtrado['nom_usina'] == usina]
    
                # Calcula a soma da geração frustrada por tipo de restrição
                geracao_por_restricao = total_por_tipo_normalizado(df_usina)
                
                # Preenche os dados para cada categoria (restrição)
                usina_dados = [geracao_por_restricao.get(r, 0) for r in categorias]
                usinas_dados.append(usina_dados)
    
            # Cria o gráfico de radar
            fig, ax = plt.subplots(figsize=(6, 6), subplot_kw=dict(polar=True))
    
            # Número de categorias (tipos de restrição)
            num_vars = len(categorias)
    
            # Ângulos para cada categoria
            angles = np.linspace(0, 2 * np.pi, num_vars, endpoint=False).tolist()
    
            # Ajusta o gráfico para que o primeiro vértice fique no topo
            angles += angles[:1]
    
            # Plotando cada usina no gráfico de radar
            for i, usina_dados in enumerate(usinas_dados):
                usina_dados += usina_dados[:1]  # Fecha o gráfico conectando o último ponto com o primeiro
                ax.plot(angles, usina_dados, label=usinas_selecionadas[i], linewidth=2)
                ax.fill(angles, usina_dados, alpha=0.25)  # Preenche a área sob a curva
    
            # Ajusta a aparência do gráfico
            ax.set_yticklabels([])  # Remove as labels do eixo radial
            ax.set_xticks(angles[:-1])  # Define as posições dos rótulos de cada categoria
            ax.set_xticklabels(categorias)  # Define os rótulos de cada categoria
    
            ax.set_title("Comparativo de Geração Frustrada por Tipo de Restrição", size=16)
    
            # Ajusta a posição da legenda para o lado direito do gráfico
            ax.legend(title="Usinas", loc='center left', bbox_to_anchor=(1.1, 0.5))
    
            plt.tight_layout()
            return fig
        else:
            fig, ax = plt.subplots()
            ax.text(0.5, 0.5, "Sem dados para exibir", ha='center', va='center')
            return fig

    
if __name__ == "__main__":
    thread_starlette = threading.Thread(target=iniciar_servidor_starlette())
    thread_shiny = threading.Thread(target=iniciar_servidor_shiny)

    # Iniciar ambas as threads
    thread_starlette.start()
    thread_shiny.start()

    # Aguardar as threads terminarem (opcional, apenas se necessário)
    thread_starlette.join()
    thread_shiny.join()

