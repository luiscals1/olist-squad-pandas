from datetime import datetime,date, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from minio import Minio

DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 13),
}

dag = DAG('etl_satisfaction_evaluation_att', 
          default_args=DEFAULT_ARGS,
          schedule_interval="@once"
        )

data_lake_server = Variable.get("data_lake_server")
data_lake_login = Variable.get("data_lake_login")
data_lake_password = Variable.get("data_lake_password")

client = Minio(
        data_lake_server,
        access_key=data_lake_login,
        secret_key=data_lake_password,
        secure=False
    )

def extract():

    #extrai os dados a partir do Data Lake.
    obj = client.get_object(
                "landing",
                "performance-evaluation/employee_performance_evaluation.json",
    )
    data = obj.read()
    df_ = pd.read_json(data,lines=True)
    
    #persiste os arquivos na área de Staging.
    df_.to_json( "/tmp/employee_performance_evaluation.json"
                ,orient="records"
                ,lines=True
                )

def load():
    from io import BytesIO

    #ler os dados a partir da área de Staging.
    df_ = pd.read_json( "/tmp/employee_performance_evaluation.json"
                ,orient="records"
                ,lines="True"
                )

    #converte os dados para o formato parquet e pesiste na área de staging.
    df_[["satisfaction_level","last_evaluation"]].to_parquet(
            "/tmp/satisfaction_evaluation.parquet"
            ,index=False
    )

    #carrega os dados para o Data Lake.
    client.fput_object(
        "processing",
        "satisfaction_evaluation.parquet",
        "/tmp/satisfaction_evaluation.parquet"
    )



extract_task = PythonOperator(
    task_id='extract_file_from_data_lake',
    provide_context=True,
    python_callable=extract,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_file_to_data_lake',
    provide_context=True,
    python_callable=load,
    dag=dag
)

clean_task = BashOperator(
    task_id="clean_files_on_staging",
    bash_command="rm -f /tmp/*.csv;rm -f /tmp/*.json;rm -f /tmp/*.parquet;",
    dag=dag
)

extract_task >> load_task >> clean_task