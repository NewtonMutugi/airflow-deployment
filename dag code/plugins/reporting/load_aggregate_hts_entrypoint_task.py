from airflow import DAG
from datetime import timedelta
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator


def build_load_aggregate_hts_entrypoint_task(dag: DAG):
    load_aggregate_hts_entrypoint_task = MsSqlOperator(task_id='load_aggregate_hts_entrypoint_task',
                                              mssql_conn_id='reporting',
                                              execution_timeout=timedelta(minutes=600),
                                              sql='sql/reporting/load_aggregate_hts_entrypoint.sql',
                                              dag=dag
                                              )
    return load_aggregate_hts_entrypoint_task
