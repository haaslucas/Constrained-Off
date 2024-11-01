import boto3
import os
from botocore import UNSIGNED
from botocore.client import Config
from datetime import datetime

# Função para baixar os arquivos de um bucket público de forma anônima
def download_files():
    
    # Nome do bucket (obtido da URL)
    bucket_name = 'ons-aws-prod-opendata'
    
    # Prefixo (caminho) onde os arquivos estão localizados
    prefix = 'dataset/restricao_coff_fotovoltaica_tm'
    # Diretório para salvar os arquivos localmente
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(base_dir, 'Arquivos', 'Fotovoltaica')    
    # Cria o diretório se não existir
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Conecta ao serviço S3 de forma anônima
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    
    # Lista todos os objetos no bucket com o prefixo dado
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    # Verifica se há arquivos
    if 'Contents' not in response:
        print('Nenhum arquivo encontrado com o prefixo especificado.')
        return

    # Ordena os arquivos pela data de modificação (último modificado por último)
    files = sorted(response['Contents'], key=lambda x: x['LastModified'])
    
    # Itera sobre os arquivos encontrados e baixa apenas os .csv
    for i, obj in enumerate(files):
        key = obj['Key']
        file_name = key.split('/')[-1]  # Obtém o nome do arquivo
        
        # Verifica se é um arquivo .csv
        if file_name.endswith('.csv'):
            file_path = os.path.join(output_dir, file_name)
            s3_last_modified = obj['LastModified'].replace(tzinfo=None)  # Remove o fuso horário da data do S3

            # Se o arquivo já existir localmente, compara as datas de modificação
            if os.path.exists(file_path):
                local_last_modified = datetime.fromtimestamp(os.path.getmtime(file_path))  # Data de modificação local
                
                # Baixa o arquivo se o do S3 for mais recente
                if s3_last_modified > local_last_modified:
                    print(f'O arquivo {file_name} foi atualizado no S3. Baixando nova versão...')
                    s3.download_file(bucket_name, key, file_path)
                    print(f'{file_name} baixado e substituído com sucesso para {file_path}')
                else:
                    print(f'{file_name} já está atualizado, ignorando...')
            else:
                # Se o arquivo não existir localmente, faz o download
                print(f'Baixando {file_name}...')
                s3.download_file(bucket_name, key, file_path)
                print(f'{file_name} baixado com sucesso para {file_path}')
        else:
            print(f'{file_name} não é um arquivo .csv, ignorando...')

# Chama a função para baixar os arquivos
download_files()
