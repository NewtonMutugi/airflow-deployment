from airflow import DAG
from datetime import timedelta
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator


def build_load_sp_generate_consistency_uploads_task(dag: DAG):
    load_sp_generate_consistency_uploads_task = MsSqlOperator(task_id='load_sp_generate_consistency_uploads_task',
                                                              mssql_conn_id='reporting',
                                                              execution_timeout=timedelta(
                                                                  minutes=600),
                                                              sql='sql/reporting/load_sp_generate_consistency_uploads_task.sql',
                                                              dag=dag
                                                              )
    return load_sp_generate_consistency_uploads_task
