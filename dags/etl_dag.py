"""
## DAG para rodar o ETL via DockerOperator

Esta DAG executa um pipeline de ETL dentro de um container Docker utilizando o DockerOperator.
"""

from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
from pendulum import datetime
from dotenv import load_dotenv
import os

load_dotenv()

# Definição da DAG
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",  # Roda todos os dias às 02:00 AM
    catchup=False
)
def firebird_to_postgres_dag():
    
    firebird_to_postgres = DockerOperator(
        task_id="run_firebird_to_postgres_container",
        image="firebird_to_postgres-python-elt",  # Nome da imagem Docker do seu ETL
        command=["poetry", "run", "python", "-u", "src/main.py"],  # Comando que roda dentro do container
        container_name="firebird_to_postgres_dag",
        docker_url="unix://var/run/docker.sock",  # Socket do Docker
        network_mode="firebird_to_postgres_elt-postgres",  # Define a rede do container
        mount_tmp_dir=False,
        auto_remove='success',
        environment= {
                'FIREBIRD_USER' : os.getenv("FIREBIRD_USER"),
                'FIREBIRD_PASSWORD': os.getenv("FIREBIRD_PASSWORD"),
                'FIREBIRD_HOST': os.getenv("FIREBIRD_HOST"),
                'FIREBIRD_PORT': os.getenv("FIREBIRD_PORT"),
                'FIREBIRD_DB_PATH': os.getenv("FIREBIRD_DB_PATH"),
                'POSTGRES_USER': os.getenv("POSTGRES_USER"),
                'POSTGRES_PASSWORD': os.getenv("POSTGRES_PASSWORD"),
                'POSTGRES_DB': os.getenv("POSTGRES_DB"),
                'POSTGRES_HOST': os.getenv("POSTGRES_HOST"),
                'POSTGRES_PORT': os.getenv("POSTGRES_PORT"),
            }
    )

    firebird_to_postgres

# Instancia a DAG
firebird_to_postgres_dag()
