from airflow import DAG
from datetime import datetime
from airflow.operators.email_operator import EmailOperator

time = datetime.now()

def build_send_ods_etl_start_email_task(dag:DAG):
    send_ods_etl_start_email = EmailOperator(
        dag = dag,
        task_id = 'send_ods_etl_start_email',
        to = ['paul.nthusi@thepalladiumgroup.com'],
        subject = 'ODS ETL Started',
        html_content = f"DWH ODS ETL started at {time}"
    )                                    
    return send_ods_etl_start_email