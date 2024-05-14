from airflow import DAG
from datetime import timedelta
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator


def build_load_aggregate_khis_hts_task(dag: DAG):
    load_load_aggregate_khis_hts_task = MsSqlOperator(task_id='load_aggregate_khis_hts_task',
                                                      mssql_conn_id='reporting',
                                                      execution_timeout=timedelta(
                                                          minutes=600),
                                                      sql='sql/reporting/load_aggregate_khis_hts.sql',
                                                      dag=dag
                                                      )
    return load_load_aggregate_khis_hts_task
