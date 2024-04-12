from datetime import datetime
from airflow import DAG
from include.extract import receber_lista_id_deputados, dados_deputados_por_id
import requests
import zipfile
from io import BytesIO

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook



dag = DAG(
    'deputados',
    schedule_interval='@daily',
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'start_date': datetime(2024, 4, 10),
        
    },
    catchup=False,
    tags=['deputados']
)

create_table_deputados = """
CREATE TABLE IF NOT EXISTS staging.deputados (
    id INTEGER,
    nome VARCHAR,
    idLegislatura INTEGER,
    siglaUF VARCHAR,
    siglaPartido VARCHAR,
    nomeEleitoral VARCHAR,
    situacao VARCHAR,
    sexo VARCHAR,
    escolaridade VARCHAR,
    dataNascimento DATE,
    dataEnvio DATE)
"""
create_table_deputados_task = PostgresOperator(
    task_id='create_table_deputados',
    postgres_conn_id='db_postgres',
    sql=create_table_deputados,
    dag=dag
)

create_table_gastos = """
CREATE TABLE IF NOT EXISTS staging.parlamentares (
    txNomeParlamentar VARCHAR(255),
    cpf VARCHAR(11),
    ideCadastro VARCHAR(255),
    nuCarteiraParlamentar VARCHAR(255),
    nuLegislatura INT,
    sgUF VARCHAR(2),
    sgPartido VARCHAR(255),
    codLegislatura INT,
    numSubCota INT,
    txtDescricao VARCHAR(255),
    numEspecificacaoSubCota INT,
    txtDescricaoEspecificacao VARCHAR(255),
    txtFornecedor VARCHAR(255),
    txtCNPJCPF VARCHAR(14),
    txtNumero VARCHAR(255),
    indTipoDocumento INT,
    datEmissao DATE,
    vlrDocumento NUMERIC(10,2),
    vlrGlosa NUMERIC(10,2),
    vlrLiquido NUMERIC(10,2),
    numMes INT,
    numAno INT,
    numParcela INT,
    txtPassageiro VARCHAR(255),
    txtTrecho VARCHAR(255),
    numLote INT,
    numRessarcimento INT,
    datPagamentoRestituicao DATE,
    vlrRestituicao NUMERIC(10,2),
    nuDeputadoId INT,
    ideDocumento INT,
    urlDocumento VARCHAR(255)
)
"""

create_table_gastos_task = PostgresOperator(
    task_id='create_table_gastos',
    postgres_conn_id='db_postgres',
    sql=create_table_gastos,
    dag=dag
)

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

extract_deputados_task = PythonOperator(
    task_id='extract_deputados_task',
    python_callable=capturando_dados_deputados,
    provide_context=True,
    dag=dag
)

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

    gastos_2023 = pd.read_csv(dados_gastos_2023)
    gastos_2024 = pd.read_csv(dados_gastos_2024)

    gastos = pd.concat(gastos_2023, gastos_2024)


    return gastos

extract_gastos_task = PythonOperator(
    task_id='extract_gastos_task',
    python_callable=capturando_dados_gastos,
    provide_context=True,
    dag=dag
)

[create_table_gastos_task, create_table_deputados_task] >> extract_deputados_task >> extract_gastos_task