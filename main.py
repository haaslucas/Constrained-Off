# -*- coding: utf-8 -*-
import subprocess

# Função para exibir a seleção do dashboard
def selecionar_dashboard():
    print("Selecione o dashboard desejado:")
    print("1 - EOLICA")
    print("2 - FOTOVOLTAICA")
    
    # Recebe a escolha do usuário
    escolha = input("Digite o numero correspondente ao dashboard desejado: ")

    return escolha

# Função para executar o script com base na escolha do usuário
def executar_dashboard(escolha):
    if escolha == "1":
        print("Executando o dashboard de Constrained off das Usinas Eolicas...")
        subprocess.run(["python", "dados.py"])
        subprocess.run(["python", "appEOL.py"])
    elif escolha == "2":
        print("Executando o dashboard Constrained off das Usinas Fotovoltaicas...")
        # Executa o script app.py
        subprocess.run(["python", "dados.py"])
        subprocess.run(["python", "app.py"])
    else:
        print("Valor invalido. Tente novamente.")

# Função principal que controla o fluxo
def inicio():
    escolha = selecionar_dashboard()  # Recebe a escolha do usuário
    executar_dashboard(escolha)  # Executa o dashboard com base na escolha

'''
# Download do banco de dados
d_coff.download_files()  # Baixa os arquivos de coff de usinas fotovoltaicas
d_coff.download_filesEOL()  # Baixa os arquivos de coff de usinas eólicas

'''

inicio()



