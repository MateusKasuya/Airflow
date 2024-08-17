from time import sleep

from airflow.decorators import dag

from datetime import datetime


@dag(
        dag_id= "minha_primeira_dag",
        description="minha etl braba",
        schedule= "* * * * *",
        start_date=datetime(2024, 8, 17),
        catchup=False #backfill

)
def pipeline():

    def primeira_atividade():
        print("minha primeira atividade - Hello World")
        sleep(2)

    def segunda_atividade():
        print("minha segunda atividade - Hello World")
        sleep(2)

    def terceira_atividade():
        print("minha terceira atividade - Hello World")
        sleep(2)

    def quarta_atividade():
        print("pipeline finalizou")

    primeira_atividade()
    segunda_atividade()
    terceira_atividade()
    quarta_atividade()