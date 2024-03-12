from airflow import DAG
from datetime import datetime, timedelta
import pendulum
from includes.superset.superset_dataset_description_trigger_task import build_superset_dataset_description_trigger_task
from includes.superset.superset_dataset_refresh_trigger_task import build_superset_dataset_refresh_trigger_task


local_tz = pendulum.timezone("Africa/Nairobi")
default_args = {
    'owner': 'kenyahmis',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 13, tzinfo=local_tz),
    'email': ['paul.nthusi@thepalladiumgroup.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'superset_background_jobs_trigger_dag',
    default_args=default_args,
    description='Superset background jobs trigger dag',
    schedule_interval='0 4 * * *',
)

dataset_description = build_superset_dataset_description_trigger_task(dag = dag)
dataset_refresh = build_superset_dataset_refresh_trigger_task(dag = dag)


dataset_description >> dataset_refresh
