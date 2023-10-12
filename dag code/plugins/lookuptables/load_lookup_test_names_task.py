from airflow import DAG
from datetime import timedelta
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator

def build_load_lookup_test_names_task(dag: DAG):
    load_lookup_test_names_task = MsSqlOperator(task_id='load_lookup_test_names_task',
                                              mssql_conn_id='ods',
                                              execution_timeout=timedelta(minutes=600),
                                              sql='sql/lkp_test_name.sql',
                                              dag=dag
                                              )
    return load_lookup_test_names_task
