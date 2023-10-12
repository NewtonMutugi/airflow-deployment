from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from includes.lookuptables.load_lookup_adverse_events_task import build_load_lookup_adverse_events_task
from includes.lookuptables.load_lookup_allergic_reaction_task import build_load_lookup_allergic_reaction_task
from includes.lookuptables.load_lookup_allergy_causative_agent_task import build_load_lookup_allergy_causative_agent_task
from includes.lookuptables.load_lookup_chronic_illness_task import build_load_lookup_chronic_illness_task
from includes.lookuptables.load_lookup_education_level_task import build_load_lookup_education_level_task
from includes.lookuptables.load_lookup_exit_reason_task import build_load_lookup_exit_reason_task
from includes.lookuptables.load_lookup_family_planning_methods_task import build_load_lookup_family_planning_methods_task
from includes.lookuptables.load_lookup_hts_disability_task import build_load_lookup_hts_disability_task
from includes.lookuptables.load_lookup_hts_strategy_task import build_load_lookup_hts_strategy_task
from includes.lookuptables.load_lookup_marital_status_task import build_load_lookup_marital_status_task
from includes.lookuptables.load_lookup_partner_offering_ovc_task import build_load_lookup_partner_offering_ovc_task
from includes.lookuptables.load_lookup_patient_sources_task import build_load_lookup_patient_sources_task
from includes.lookuptables.load_lookup_prophylaxis_types_task import build_load_lookup_prophylaxis_types_task
from includes.lookuptables.load_lookup_pwp_task import build_load_lookup_pwp_task
from includes.lookuptables.load_lookup_regimen_map_task import build_load_lookup_regimen_map_task
from includes.lookuptables.load_lookup_regimen_task import build_load_lookup_regimen_task
from includes.lookuptables.load_lookup_test_names_task import build_load_lookup_test_names_task
from includes.lookuptables.load_lookup_treatment_types_task import build_load_lookup_treatment_types_task



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

dag = DAG(dag_id='lookup_tables_etl_dag',
          schedule_interval=None,
          default_args=default_args,
          )

load_lookup_adverse_events = build_load_lookup_adverse_events_task(dag = dag)
load_lookup_allergic_reaction = build_load_lookup_allergic_reaction_task(dag = dag)
load_lookup_allergy_causative_agent = build_load_lookup_allergy_causative_agent_task(dag = dag)
load_lookup_chronic_illness = build_load_lookup_chronic_illness_task(dag = dag)
load_lookup_education_level = build_load_lookup_education_level_task(dag = dag)
load_lookup_exit_reason = build_load_lookup_exit_reason_task(dag = dag)
load_lookup_family_planning_methods = build_load_lookup_family_planning_methods_task(dag = dag)
load_lookup_hts_disability = build_load_lookup_hts_disability_task(dag = dag)
load_lookup_hts_strategy = build_load_lookup_hts_strategy_task(dag = dag)
load_lookup_marital_status = build_load_lookup_marital_status_task(dag = dag)
load_lookup_partner_offering_ovc = build_load_lookup_partner_offering_ovc_task(dag = dag)
load_lookup_patient_sources = build_load_lookup_patient_sources_task(dag = dag)
load_lookup_prophylaxis_types = build_load_lookup_prophylaxis_types_task(dag = dag)
load_lookup_pwp = build_load_lookup_pwp_task(dag = dag)
load_lookup_regimen_map = build_load_lookup_regimen_map_task(dag = dag)
load_lookup_regimen = build_load_lookup_regimen_task(dag = dag)
load_lookup_test_names = build_load_lookup_test_names_task(dag = dag)
load_lookup_treatment_types = build_load_lookup_treatment_types_task(dag = dag)

load_lookup_adverse_events >> load_lookup_allergic_reaction >> load_lookup_allergy_causative_agent 
load_lookup_allergy_causative_agent >> load_lookup_chronic_illness >> load_lookup_education_level >> load_lookup_exit_reason
load_lookup_exit_reason >> load_lookup_family_planning_methods >> load_lookup_hts_disability
load_lookup_hts_disability >> load_lookup_hts_strategy >> load_lookup_marital_status >> load_lookup_partner_offering_ovc
load_lookup_partner_offering_ovc >> load_lookup_patient_sources >> load_lookup_prophylaxis_types >> load_lookup_pwp
load_lookup_pwp >> load_lookup_regimen_map >> load_lookup_regimen >> load_lookup_test_names >> load_lookup_treatment_types












