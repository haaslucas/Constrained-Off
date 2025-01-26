from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn
from graficos import gerar_mapa
from app import df

app = FastAPI()

# Função para gerar a interface com filtros
@app.get("/", response_class=HTMLResponse)
def exibir_mapa(estado: str = "Todos", tipo_restricao: str = "Todos", usina: str = "Todos"):
    # Gera o mapa baseado nos filtros
    mapa_gerado = gerar_mapa(df, estado=estado, tipo_restricao=tipo_restricao, usina=usina)
    
    # Salva o arquivo HTML atualizado
    with open(mapa_gerado, "r", encoding="utf-8") as file:
        html_content = file.read()
    
    return HTMLResponse(content=html_content)

# Rota para retornar as opções de filtros
@app.get("/filtros", response_class=HTMLResponse)
def obter_filtros():
    estados = ["Todos"] + list(df['nom_estado'].dropna().unique())
    restricoes = ["Todos"] + list(df['cod_razaorestricao'].dropna().unique())
    
    # HTML com JavaScript para atualizar o filtro de usinas
    return f"""
    <html>
        <head>
            <title>Filtros</title>
            <script>
                async function atualizarUsinas() {{
                    const estadoSelecionado = document.getElementById("estado").value;
                    const response = await fetch(`/usinas?estado=${{estadoSelecionado}}`);
                    const data = await response.json();
                    const usinasSelect = document.getElementById("usina");
                    usinasSelect.innerHTML = ""; // Limpa as opções existentes
                    data.usinas.forEach(usina => {{
                        const option = document.createElement("option");
                        option.value = usina;
                        option.textContent = usina;
                        usinasSelect.appendChild(option);
                    }});
                }}
            </script>
        </head>
        <body>
            <h1>Filtros do mapeamento geografico dos cortes</h1>
            <form method="get" action="/">
                <label for="estado">Estado:</label>
                <select name="estado" id="estado" onchange="atualizarUsinas()">
                    {''.join([f'<option value="{estado}">{estado}</option>' for estado in estados])}
                </select><br><br>

                <label for="tipo_restricao">Tipo de Restrição:</label>
                <select name="tipo_restricao" id="tipo_restricao">
                    {''.join([f'<option value="{restricao}">{restricao}</option>' for restricao in restricoes])}
                </select><br><br>

                <label for="usina">Usina:</label>
                <select name="usina" id="usina">
                    <option value="Todos">Todos</option>
                </select><br><br>

                <input type="submit" value="Filtrar">
            </form>
        </body>
    </html>
    """

# Rota para obter as usinas com base no estado selecionado
@app.get("/usinas", response_class=JSONResponse)
def obter_usinas(estado: str):
    if estado == "Todos":
        usinas = ["Todos"] + list(df['nom_usina'].dropna().unique())
    else:
        usinas = ["Todos"] + list(df[df['nom_estado'] == estado]['nom_usina'].dropna().unique())
    return {"usinas": usinas}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8001)
