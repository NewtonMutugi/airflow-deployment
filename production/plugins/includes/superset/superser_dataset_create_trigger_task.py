from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator


def build_superset_dataset_create_trigger_task(dag: DAG):
    superset_dataset_create_trigger = SimpleHttpOperator(
        task_id='superset_dataset_create_trigger',
        http_conn_id='superset_http',
        endpoint='/api/dataset/refresh',
        method='PUT',
        headers={"Content-Type": "application/json"},
        dag=dag,
    )

    return superset_dataset_create_trigger
