import os
import sys
import zipfile
from typing import List
from io import BytesIO


import requests

url = "https://dadosabertos.camara.leg.br/api/v2/deputados"

def receber_lista_id_deputados():
    """Receber lista de id deputados."""
    response = requests.get(url)

    try:
        if response.status_code == 200:
            data = response.json()
            deputados_id = [deputado["id"] for deputado in data["dados"]]
            return deputados_id
    except Exception as e:
        print(f"Error ao se conectart com API:{e}")
        return None


def dados_deputados_por_id(deputados_id: List[int]):
    """
    Receber dados deputados por id.

    Args:
        deputados_id (List[int]): Lista de id deputados.
    """
    response = requests.get(f"{url}/{deputados_id}")

    try:
        if response.status_code == 200:
            data = response.json()
            return data["dados"]
    except Exception as e:
        print(f"Error ao se conectart com API:{e}")
        return None


def capturando_dados_deputados():
    """Capturar dados deputados."""
    dados = []
    deputados_id = receber_lista_id_deputados()

    for id in deputados_id:
        dados_deputados = dados_deputados_por_id(id)
        if dados_deputados:
            dado = {
                "id": dados_deputados["id"],
                "nome": dados_deputados["ultimoStatus"]["nome"],
                "idLegislatura": dados_deputados["ultimoStatus"]["idLegislatura"],
                "siglaUF": dados_deputados["ultimoStatus"]["siglaUf"],
                "siglaPartido": dados_deputados["ultimoStatus"]["siglaPartido"],
                "nomeEleitoral": dados_deputados["ultimoStatus"]["nomeEleitoral"],
                "situacao": dados_deputados["ultimoStatus"]["situacao"],
                "sexo": dados_deputados["sexo"],
                "escolaridade": dados_deputados["escolaridade"],
                "dataNascimento": dados_deputados["dataNascimento"],
            }
            dados.append(dado)

    return dados


def capturando_dados_gastos():
    url_gastos_2024 = "https://www.camara.leg.br/cotas/Ano-2024.csv.zip"
    ult_gastos_2023 = "https://www.camara.leg.br/cotas/Ano-2023.csv.zip"

    def baixar_arquivos_zip(url):
        response = requests.get(url)
        if response.status_code == 200:
            with zipfile.ZipFile(BytesIO(response.content), "r") as zip_ref:
                arquivo = zip_ref.namelist()[0]  # Obtém o nome do arquivo dentro do zip
                return zip_ref.read(arquivo)  # Retorna o conteúdo do arquivo
        else:
            print(f"Falha ao baixar o arquivo na url {url}: Código de status: {response.status_code}")
            return None

    dados_gastos_2024 = baixar_arquivos_zip(url_gastos_2024)
    dados_gastos_2023 = baixar_arquivos_zip(ult_gastos_2023)

    return dados_gastos_2024, dados_gastos_2023

if __name__ == '__main__':
    capturando_dados_gastos()