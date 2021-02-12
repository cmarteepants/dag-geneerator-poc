from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import pickle


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def create_and_pickle_dag():

    with open("include/job_types.txt") as f:
        jobs = f.read().split(", ")

        for job in jobs:
            dag = DAG(
                dag_id=f'{job}_dag',
                start_date=datetime(2021, 2, 9),
                max_active_runs=3,
                schedule_interval="0 10 * * *",
                default_args=default_args
            )

            dummy_id = DummyOperator(
                task_id=f"trigger_spark_job",
                dag=dag
            )

            with open(f"{dag.dag_id}.pkl", "wb") as f:
                pickle.dump(dag, f)


with DAG(
    dag_id="dag_generator",
    schedule_interval=None,
    max_active_runs=3,
    default_args=default_args
) as dag:

    dag_generator = PythonOperator(
        task_id="create_dags",
        python_callable=create_and_pickle_dag
    )
