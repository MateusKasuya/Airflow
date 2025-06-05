from airflow.decorators import dag
from airflow.providers.docker.operators.docker import DockerOperator
from pendulum import datetime
from dotenv import load_dotenv
import os

load_dotenv()

@dag(
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    tags=["etl", "dbt"]
)
def etl_with_dbt():
    
    firebird_to_postgres = DockerOperator(
        task_id="run_firebird_to_postgres_container",
        image="firebird_to_postgres-python-elt",
        command=["poetry", "run", "python", "-u", "src/main.py"],
        container_name="firebird_to_postgres_dag",
        docker_url="unix://var/run/docker.sock",
        network_mode="firebird_to_postgres_elt-postgres",
        mount_tmp_dir=False,
        auto_remove="success",
        environment={
            'FIREBIRD_USER': os.getenv("FIREBIRD_USER"),
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

    dbt_postgres = DockerOperator(
        task_id="run_dbt_postgres_container",
        image="dbt-dbt_softcenter",
        command=["poetry", "run", "dbt", "run"],
        container_name="dbt_postgres_dag",
        docker_url="unix://var/run/docker.sock",
        network_mode="firebird_to_postgres_elt-postgres",
        mount_tmp_dir=False,
        auto_remove="success",
        do_xcom_push=False,
        environment={
            'DBT_PROFILES_DIR': os.getenv("DBT_PROFILES_DIR"),
            'DBT_PROFILES_DBNAME': os.getenv("DBT_PROFILES_DBNAME"),
            'DBT_PROFILES_HOST': os.getenv("DBT_PROFILES_HOST"),
            'DBT_PROFILES_PASSWORD': os.getenv("DBT_PROFILES_PASSWORD"),
            'DBT_PROFILES_PORT': os.getenv("DBT_PROFILES_PORT"),
            'DBT_PROFILES_SCHEMA': os.getenv("DBT_PROFILES_SCHEMA"),
            'DBT_PROFILES_THREADS': os.getenv("DBT_PROFILES_THREADS"),
            'DBT_PROFILES_USER': os.getenv("DBT_PROFILES_USER"),
        }
    )

    firebird_to_postgres >> dbt_postgres

dag = etl_with_dbt()
