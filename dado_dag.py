from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from dags.programs.dado_etl import arrojar_dado, elegir_dado_mas_alto

from datetime import datetime


with DAG("dado_dag", start_date=datetime(2022, 1, 1),
    schedule_interval="@daily", catchup=False) as dag:

        arrojar_dados = [PythonOperator(
            task_id=f'dado_{task}',
            python_callable=arrojar_dado
            ) for task in ['A', 'B', 'C']]

        dado_mas_alto = BranchPythonOperator(
            task_id="dado_mas_alto",
            python_callable=elegir_dado_mas_alto
        )

        es_par = BashOperator(
            task_id="es_par",
            bash_command="echo 'El dado {{ti.xcom_pull(key='dado_max_value', task_ids=['dado_mas_alto']) }} es par'"
        )

        es_impar = BashOperator(
            task_id="es_impar",
            bash_command="echo 'El dado {{ti.xcom_pull(key='dado_max_value', task_ids=['dado_mas_alto']) }} es impar'"
        )

        arrojar_dados >> dado_mas_alto >> [es_par, es_impar]
