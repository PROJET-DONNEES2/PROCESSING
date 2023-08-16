from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from Demeter import download_files
from Apply_aphrodite import apply_aphrodite
from Hera_Alpha import append_same_sources_df
from Hera_Beta import append_all


def download_adzuna_files(**kwargs):
    aws_access_key = Variable.get("AWS_ACCESS_KEY")
    aws_secret_key = Variable.get("AWS_SECRET_KEY")
    region_name = Variable.get("AWS_REGION")

    download_files(aws_access_key=aws_access_key, aws_secret_key=aws_secret_key,
                   region_name=region_name, folder="adzuna", prefix="adzuna")


def download_pole_emploi_files(**kwargs):
    aws_access_key = Variable.get("AWS_ACCESS_KEY")
    aws_secret_key = Variable.get("AWS_SECRET_KEY")
    region_name = Variable.get("AWS_REGION")

    download_files(aws_access_key=aws_access_key, aws_secret_key=aws_secret_key,
                   region_name=region_name, folder="pole-emploi", prefix="/tmp")


with DAG(
    "Job-analysis",
    default_args={
        "depends_on_past": False,
        "email": ["hei.onitsiky@gmail.com", "hei.sanda.2@gmail.com", "hei.dinasoa@gmail.com"],
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=1)
    },
    description="First DAG try",
    schedule=timedelta(hours=3),
    start_date=datetime(2022, 1, 1, 0, 0, 0),
    catchup=False,
    tags=["env=all"],
) as dag:
    t1 = PythonOperator(
        task_id="Download_Adzuna_datas",
        python_callable=download_adzuna_files,
        provide_context=True,
        dag=dag,
        retries=1
    )

    t2 = PythonOperator(
        task_id="Download_Pole_Emploi_datas",
        python_callable=download_pole_emploi_files,
        provide_context=True,
        dag=dag,
        retries=1
    )

    t3 = PythonOperator(
        task_id="Clean_datas",
        python_callable=apply_aphrodite,
        provide_context=True,
        dag=dag,
        retries=1
    )

    t4 = PythonOperator(
        task_id="Merge_partially",
        python_callable=append_same_sources_df,
        provide_context=True,
        dag=dag,
        retries=1
    )

    t5 = PythonOperator(
        task_id="Merge_all",
        python_callable=append_all,
        provide_context=True,
        dag=dag,
        retries=1
    )

    [t1, t2] >> t3 >> t4 >> t5