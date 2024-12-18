import boto3
import os
from botocore import UNSIGNED
from botocore.client import Config
from datetime import datetime
import pytz

class DownloadManager:
    def __init__(self):
        self.downloadrealizadoFV = False
        self.downloadrealizadoEOL = False

    def download_files(self):
        # Nome do bucket (obtido da URL)
        bucket_name = 'ons-aws-prod-opendata'
        prefix = 'dataset/restricao_coff_fotovoltaica_tm'

        base_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.join(base_dir, 'Arquivos', 'Fotovoltaica')
        os.makedirs(output_dir, exist_ok=True)

        s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        if 'Contents' not in response:
            print('Nenhum arquivo encontrado com o prefixo especificado.')
            return

        files = sorted(response['Contents'], key=lambda x: x['LastModified'])

        for obj in files:
            key = obj['Key']
            file_name = key.split('/')[-1]

            if file_name.endswith('.csv'):
                file_path = os.path.join(output_dir, file_name)
                s3_last_modified = obj['LastModified'].replace(tzinfo=pytz.UTC)

                if os.path.exists(file_path):
                    local_last_modified = datetime.fromtimestamp(
                        os.path.getmtime(file_path), tz=pytz.UTC
                    )

                    if s3_last_modified > local_last_modified:
                        print(f'O arquivo {file_name} foi atualizado no S3. Baixando nova versão...')
                        s3.download_file(bucket_name, key, file_path)
                        self.downloadrealizadoFV = True
                        print(f'{file_name} baixado e substituído com sucesso para {file_path}')
                    else:
                        print(f'{file_name} já está atualizado, ignorando...')
                else:
                    print(f'Baixando {file_name}...')
                    s3.download_file(bucket_name, key, file_path)
                    self.downloadrealizadoFV = True
                    print(f'{file_name} baixado com sucesso para {file_path}')
            else:
                print(f'{file_name} não é um arquivo .csv, ignorando...')

    def download_filesEOL(self):
        # Nome do bucket (obtido da URL)
        bucket_name = 'ons-aws-prod-opendata'
        prefix = 'dataset/restricao_coff_eolica_tm'

        base_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.join(base_dir, 'Arquivos', 'Eólica')
        os.makedirs(output_dir, exist_ok=True)

        s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        if 'Contents' not in response:
            print('Nenhum arquivo encontrado com o prefixo especificado.')
            return

        files = sorted(response['Contents'], key=lambda x: x['LastModified'])

        for obj in files:
            key = obj['Key']
            file_name = key.split('/')[-1]

            if file_name.endswith('.csv'):
                file_path = os.path.join(output_dir, file_name)
                s3_last_modified = obj['LastModified'].replace(tzinfo=pytz.UTC)

                if os.path.exists(file_path):
                    local_last_modified = datetime.fromtimestamp(
                        os.path.getmtime(file_path), tz=pytz.UTC
                    )

                    if s3_last_modified > local_last_modified:
                        print(f'O arquivo {file_name} foi atualizado no S3. Baixando nova versão...')
                        s3.download_file(bucket_name, key, file_path)
                        self.downloadrealizadoEOL = True
                        print(f'{file_name} baixado e substituído com sucesso para {file_path}')
                    else:
                        print(f'{file_name} já está atualizado, ignorando...')
                else:
                    print(f'Baixando {file_name}...')
                    s3.download_file(bucket_name, key, file_path)
                    self.downloadrealizadoEOL = True
                    print(f'{file_name} baixado com sucesso para {file_path}')
            else:
                print(f'{file_name} não é um arquivo .csv, ignorando...')

# Exemplo de uso:
# manager = DownloadManager()
# manager.download_files()
# manager.download_filesEOL()
# print(manager.downloadrealizadoFV)
# print(manager.downloadrealizadoEOL)
