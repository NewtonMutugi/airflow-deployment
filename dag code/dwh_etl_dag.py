from datetime import datetime, timedelta
import imp
from statistics import variance
import pendulum
from airflow import DAG
from includes.load_all_facilities_task import build_load_all_facilities_task
from includes.load_art_patients_task import build_load_art_patients_task
from includes.load_ct_patient_visits_task import build_load_ct_patient_visits_task
from includes.load_adverse_events_task import build_load_adverse_events_task
from includes.load_ct_patient_labs_task import build_load_patient_labs_task
from includes.load_ct_patient_status_task import build_load_ct_patient_status_task
from includes.load_ct_patients_task import build_load_ct_patients_task
from includes.load_patient_pharmacy_task import build_load_patient_pharmacy_task

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
dag = DAG(dag_id='dwh_etl_dag',
          default_args=default_args,
          catchup=False,
          schedule_interval="0 12 15 * *")

load_facilities = build_load_all_facilities_task(dag = dag)
load_art_patients = build_load_art_patients_task(dag = dag)
load_ct_patients = build_load_ct_patient_visits_task(dag = dag)
load_adverse_events = build_load_adverse_events_task(dag = dag)
load_patient_labs = build_load_patient_labs_task(dag = dag)
load_ct_patient_status = build_load_ct_patient_status_task(dag = dag)
load_ct_patients = build_load_ct_patients_task(dag = dag)
load_patient_pharmacy = build_load_patient_pharmacy_task(dag = dag)

load_facilities >> load_ct_patients >>load_art_patients >> load_ct_patients >> load_adverse_events >> load_patient_labs >> load_ct_patient_status >> load_patient_pharmacy 