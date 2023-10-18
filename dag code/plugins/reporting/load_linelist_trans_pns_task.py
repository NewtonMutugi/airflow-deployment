from airflow import DAG
from datetime import timedelta
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator


def build_load_linelist_trans_pns_task(dag: DAG):
    load_linelist_trans_pns_task = MsSqlOperator(task_id='load_linelist_trans_pns_task',
                                              mssql_conn_id='reporting',
                                              execution_timeout=timedelta(minutes=600),
                                              sql='sql/reporting/load_linelist_trans_pns.sql',
                                              dag=dag
                                              )
    return load_linelist_trans_pns_task
