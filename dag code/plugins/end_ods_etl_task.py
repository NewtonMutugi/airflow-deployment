from airflow import DAG
from datetime import datetime
from airflow.operators.email_operator import EmailOperator

time = datetime.now()

def build_send_ods_etl_end_email_task(dag:DAG):
    send_ods_etl_end_email = EmailOperator(
        dag = dag,
        task_id = 'send_ods_etl_end_email',
        to = ['paul.nthusi@thepalladiumgroup.com'],
        subject = 'ODS ETL Complete',
        html_content = f"DWH ODS ETL ended at {time}"
    )                                    
    return send_ods_etl_end_email