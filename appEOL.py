from graficos import (
    total_por_estado,
    percentuais_por_tipo, percentuais_por_estado, percentuais_por_regiao, corte_por_estado, dfEOL
)
import matplotlib.pyplot as plt
from shiny import run_app
import pandas as pd
from shiny import App, render, reactive, ui

app_ui = ui.page_fluid(
    ui.h2("Constrained Off de Usinas Eólicas - Dados Abertos ONS"),
    
    # Filtros
    ui.input_select("estado", "Selecione o Estado:", choices=["Todos"] + list(dfEOL['nom_estado'].unique())),
    ui.input_select("tipo_restricao", "Selecione a Restrição:", choices=["Todos"] + list(dfEOL['cod_razaorestricao'].dropna().unique())),
    ui.input_select("usina", "Selecione a Usina:", choices=["Todos"]),

    ui.row(
        ui.column(4, ui.output_plot("por_tipo")),     
        ui.column(4, ui.output_plot("por_regiao")),  
        ui.column(4, ui.output_plot("por_estado"))
    ),

    # Output para mostrar gráficos ou tabelas
    ui.output_plot("total"),
    ui.output_plot("total_estado"),
    ui.output_plot("corte_estado"),
    ui.output_plot("media_diaria")
 
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
        media_agrupada = media.groupby(['Mes', 'cod_razaorestricao'])['Geracao limitada MWh'].sum().unstack(fill_value=0)

        # Configurando o gráfico
        fig, ax = plt.subplots(figsize=(12, 6))
        media_agrupada.plot(kind='bar', ax=ax, stacked=True, width=0.8)

        # Títulos e rótulos
        ax.set_title('Total de Geração Limitada por Mês e Tipo de Restrição')
        ax.set_xlabel('Mês')
        ax.set_ylabel('Geração Limitada [MWh]')
        
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
        df_pivot = media.pivot_table(index='Mes', columns='nom_estado', values='Geracao limitada MWh', aggfunc='sum')
    
        # Configuração do gráfico com matplotlib
        fig, ax = plt.subplots(figsize=(12, 7))
        df_pivot.plot(kind='bar', ax=ax, stacked=False, width=0.8)
    
        # Ajustes do gráfico
        ax.set_title('Total de Geração Limitada por Estado e Mês - Valores Absolutos')
        ax.set_xlabel('Mês')
        ax.set_ylabel('Geração Limitada [MWh]')
        
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
        ax.set_title('Média de Geração Limitada por Estado e Mês - Valores %')
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
                'val_geracaolimitada': 'mean',  
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
                'val_geracaolimitada': 'mean',  
                'val_disponibilidade': 'mean',    
                'val_geracaoreferencia': 'mean',  
                'val_geracao': 'mean'
            }).reset_index()          
            media['Hora'] = pd.to_datetime(media['Hora'].astype(str), format='%H:%M:%S')
            media['Hora'] = media['Hora'].dt.strftime('%H:%M')

        # Criar o gráfico
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Plotar as linhas para cada coluna desejada
        ax.plot(media['Hora'], media['val_geracaolimitada'], label='Geração Limitada', marker='o')
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
        sizes = dados_pizza['val_geracaolimitada']

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
        sizes = dados_pizza['val_geracaolimitada']

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
        sizes = dados_pizza['val_geracaolimitada']

        fig, ax = plt.subplots()
        ax.pie(sizes, labels=labels, autopct=lambda p: f'{p:.1f}%', startangle=90)
        ax.axis('equal')
        
        return fig
    
# Inicializa o app
app = App(app_ui, server)
# Roda o app se esse código está sendo executado diretamente
if __name__ == "__main__":
    run_app(app)
