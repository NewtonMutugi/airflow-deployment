from airflow import DAG
from datetime import datetime, timedelta
import pendulum
from includes.superset.superset_dataset_description_trigger_task import build_superset_dataset_description_trigger_task
from includes.superset.superset_dataset_refresh_trigger_task import build_superset_dataset_refresh_trigger_task
from includes.superset.superset_dataset_create_trigger_task import build_superset_dataset_create_trigger_task


local_tz = pendulum.timezone("Africa/Nairobi")
default_args = {
    'owner': 'kenyahmis',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 4, tzinfo=local_tz),
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
    catchup=False,
    schedule_interval='0 2 * * 1,3',
)
dataset_create = build_superset_dataset_create_trigger_task(dag = dag)
dataset_description = build_superset_dataset_description_trigger_task(dag = dag)
dataset_refresh = build_superset_dataset_refresh_trigger_task(dag = dag)


dataset_create >> dataset_refresh >> dataset_description
