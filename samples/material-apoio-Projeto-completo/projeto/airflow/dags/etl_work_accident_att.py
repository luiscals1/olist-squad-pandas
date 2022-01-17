from datetime import datetime,date, timedelta
import math
import pandas as pd
from io import BytesIO
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from minio import Minio
from sqlalchemy.engine import create_engine


DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 13),
}

dag = DAG('etl_work_accident_att', 
          default_args=DEFAULT_ARGS,
          schedule_interval="@once"
        )

data_lake_server = Variable.get("data_lake_server")
data_lake_login = Variable.get("data_lake_login")
data_lake_password = Variable.get("data_lake_password")

database_server = Variable.get("database_server")
database_login = Variable.get("database_login")
database_password = Variable.get("database_password")
database_name = Variable.get("database_name")


url_connection = "mysql+pymysql://{}:{}@{}/{}".format(
                 str(database_login)
                ,str(database_password)
                ,str(database_server)
                ,str(database_name)
                )

engine = create_engine(url_connection)

client = Minio(
        data_lake_server,
        access_key=data_lake_login,
        secret_key=data_lake_password,
        secure=False
    )

def extract():

    #obtendo toda a tabela employees e armazenando como um dataframe.
    df_employees = pd.read_sql_table("employees",engine)

    #obtendo toda a tabela accident e armazenando como um dataframe.
    df_accident = pd.read_sql_table("accident",engine)
    
    #verificando empregados que sofreram algum acidente.
    work_accident = []
    for emp in df_employees["emp_no"]:
        if emp in df_accident["emp_no"].to_list():
            work_accident.append(1)
        else:
            work_accident.append(0)
    
    #criando a estrutura do Dataframe temporário e atribuindo os dados.
    df_ = pd.DataFrame(data=None, columns=["work_accident"])
    df_["work_accident"] = work_accident
    
    #persiste os arquivos na área de Staging.
    df_.to_csv("/tmp/work_accident.csv"
               ,index=False
            )

def load():

    #carrega os dados a partir da área de staging.
    df_ = pd.read_csv("/tmp/work_accident.csv")

    #converte os dados para o formato parquet.    
    df_.to_parquet(
            "/tmp/work_accident.parquet"
            ,index=False
    )

    #carrega os dados para o Data Lake.
    client.fput_object(
        "processing",
        "work_accident.parquet",
        "/tmp/work_accident.parquet"
    )


extract_task = PythonOperator(
    task_id='extract_data_from_database',
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