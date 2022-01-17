import datetime
from io import BytesIO
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from minio import Minio

DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 1, 13),
}

dag = DAG('etl_mean_work_last_3_months_att', 
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

    #cria a estrutura para o dataframe temporário.
    df_working_hours = pd.DataFrame(data=None, columns=["emp_id","data","hora"])
    
    #list objects

    objects = client.list_objects('landing', prefix='working-hours',
                              recursive=True)
    for obj in objects:

        print("Downloading file...")
        print(obj.bucket_name, obj.object_name.encode('utf-8'))

        obj = client.get_object(
                obj.bucket_name,
                obj.object_name.encode('utf-8'),
        )
        data = obj.read()
        df_ = pd.read_excel(data)
        df_working_hours = pd.concat([df_working_hours,df_])
        
    #persiste o dataset na área de Staging.
    df_working_hours.to_csv( "/tmp/mean_work_last_3_months.csv"
                ,index=False
            )
 
def transform():

    #ler os dados a partir da área de Staging.
    df_ = pd.read_csv( "/tmp/mean_work_last_3_months.csv")
    
    #converte os dados para o formato numeric e datetime.
    df_["hora"] = pd.to_numeric(df_["hora"])
    df_["data"] = pd.to_datetime(df_["data"])    
    
    #filtra apenas os registros dos ultimos 3 meses.
    df_last_3_month = df_[(df_['data'] > datetime.datetime(2020,9,30))]
    
    #calcula as horas de trabalho média nos ultimos meses.
    mean_work_last_3_months = df_last_3_month.groupby("emp_id")["hora"].agg("sum")/3

    #cria o dataframe com os dados transformados.
    mean_work_last_3_months = pd.DataFrame(data=mean_work_last_3_months)
    mean_work_last_3_months.rename(columns={"hora":"mean_work_last_3_months"},inplace=True)

    #persiste os dados transformados na área de staging.
    mean_work_last_3_months.to_csv(
        "/tmp/mean_work_last_3_months.csv"
        ,index=False
    )

def load():
    
    #carrega os dados a partir da área de staging.
    df_ = pd.read_csv("/tmp/mean_work_last_3_months.csv")

    #converte os dados para o formato parquet.
    df_.to_parquet(
        "/tmp/mean_work_last_3_months.parquet"
        ,index=False
    )

    #carrega os dados para o Data Lake.
    client.fput_object(
        "processing",
        "mean_work_last_3_months.parquet",
        "/tmp/mean_work_last_3_months.parquet"
    )


extract_task = PythonOperator(
    task_id='extract_file_from_data_lake',
    provide_context=True,
    python_callable=extract,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    provide_context=True,
    python_callable=transform,
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

extract_task >> transform_task >> load_task >> clean_task