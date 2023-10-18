from airflow import DAG
from datetime import timedelta
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator


def build_load_linelist_viralload_task(dag: DAG):
    load_linelist_viralload_task = MsSqlOperator(task_id='load_linelist_viralload_task',
                                              mssql_conn_id='reporting',
                                              execution_timeout=timedelta(minutes=600),
                                              sql='sql/reporting/load_linelist_viralload.sql',
                                              dag=dag
                                              )
    return load_linelist_viralload_task
