from datetime import datetime, timedelta
from airflow.models import Variable
import pendulum
from airflow import DAG
from includes.load_dim_age_group_task import build_age_group_dimension
from includes.load_dim_agency_task import build_load_dim_agency
from includes.load_dim_art_outcome_task import build_load_dim_art_outcome
from includes.load_dim_date_task import build_load_date_dimension
from includes.load_dim_differentiated_care_task import build_differentiated_care_dimension
from includes.load_dim_drug_task import build_drug_dimension
from includes.load_dim_facilities_task import build_load_dim_facilities
from includes.load_dim_hts_trace_outcome_task import build_load_dim_hts_trace_outcome
from includes.load_dim_hts_trace_type_task import build_load_dim_hts_trace_type
from includes.load_dim_partners_task import build_load_dim_partners
from includes.load_dim_patients_task import build_load_dim_patients
from includes.load_dim_regimen_line_task import build_regimen_line_dimension
from includes.load_dim_relation_to_patient_task import build_relation_to_patient_dimension
from includes.load_dim_test_kit_name_task import build_load_dim_test_kit_name
from includes.load_dim_treatment_type_task import build_load_dim_treatment_type

local_tz = pendulum.timezone("Africa/Nairobi")

spark_app_home = Variable.get("SPARK_APP_HOME")
spark_driver_port = Variable.get("SPARK_DRIVER_PORT")
spark_driver_block_manager_port = Variable.get("SPARK_DRIVER_BLOCK_MANAGER_PORT")
spark_driver_host = Variable.get("SPARK_DRIVER_HOST")
spark_driver_bind_address = Variable.get("SPARK_DRIVER_BIND_ADDRESS")
default_spark_total_executor_cores = Variable.get("DEFAULT_SPARK_TOTAL_EXECUTOR_CORES")
default_spark_executor_cores = Variable.get("DEFAULT_SPARK_EXECUTOR_CORES")
default_spark_executor_memory = Variable.get("DEFAULT_SPARK_EXECUTOR_MEMORY")
default_spark_driver_memory = Variable.get("DEFAULT_SPARK_DRIVER_MEMORY")

spark_default_numpartitions = Variable.get("SPARK_DEFAULT_NUMPARTITIONS")
spark_dwapicentral_url = Variable.get("SPARK_DWAPICENTRAL_URL")
spark_dwapicentral_driver = Variable.get("SPARK_DWAPICENTRAL_DRIVER")
spark_dwapicentral_user = Variable.get("SPARK_DWAPICENTRAL_USER")
spark_dwapicentral_password = Variable.get("SPARK_DWAPICENTRAL_PASSWORD")
spark_ods_numpartitions = Variable.get("SPARK_ODS_NUMPARTITIONS")
spark_ods_url = Variable.get("SPARK_ODS_URL")
spark_ods_driver = Variable.get("SPARK_ODS_DRIVER")
spark_ods_user = Variable.get("SPARK_ODS_USER")
spark_ods_password = Variable.get("SPARK_ODS_PASSWORD")
spark_edw_url = Variable.get("SPARK_EDW_URL")
spark_edw_driver = Variable.get("SPARK_EDW_DRIVER")
spark_edw_user = Variable.get("SPARK_EDW_USER")
spark_edw_password = Variable.get("SPARK_EDW_PASSWORD")
spark_dim_agency_table = Variable.get("SPARK_DIM_AGENCY_TABLE")
spark_dim_age_group_table = Variable.get("SPARK_DIM_AGE_GROUP_TABLE")
spark_dim_art_outcome_table = Variable.get("SPARK_DIM_ART_OUTCOME_TABLE")
spark_dim_date_table = Variable.get("SPARK_DIM_DATE_TABLE")
spark_dim_facility_table = Variable.get("SPARK_DIM_FACILITY_TABLE")
spark_dim_partner_table = Variable.get("SPARK_DIM_PARTNER_TABLE")
spark_dim_patient_table = Variable.get("SPARK_DIM_PATIENT_TABLE")
spark_dim_drug_table = Variable.get("SPARK_DIM_DRUG_TABLE")
spark_dim_differentiated_care_table = Variable.get("SPARK_DIM_DIFFERENTIATED_CARE_TABLE")
spark_dim_relationship_with_patient_table = Variable.get("SPARK_DIM_RELATIONSHIP_WITH_PATIENT_TABLE")
spark_dim_regimen_line_table = Variable.get("SPARK_DIM_REGIMEN_LINE_TABLE")
spark_historical_art_outcome_base_table = Variable.get("SPARK_HISTORICAL_ART_OUTCOME_BASE_TABLE")
spark_fact_art_history_table = Variable.get("SPARK_FACT_ART_HISTORY_TABLE")
spark_fact_art_table = Variable.get("SPARK_FACT_ART_TABLE")
spark_fact_otz_table = Variable.get("SPARK_FACT_OTZ_TABLE")
spark_fact_ovc_table = Variable.get("SPARK_FACT_OVC_TABLE")
spark_fact_latest_obs_table = Variable.get("SPARK_FACT_LATEST_OBS_TABLE")
spark_fact_covid_table = Variable.get("SPARK_FACT_COVID_TABLE")
spark_intermediate_art_outcomes_table = Variable.get("SPARK_INTERMEDIATE_ART_OUTCOMES_TABLE")

default_conf = {
    "connection_id": "spark_standalone",
    "spark_app_home":spark_app_home,
    "default_spark_total_executor_cores": default_spark_total_executor_cores,
    "default_spark_executor_cores": default_spark_executor_cores,
    "default_spark_executor_memory": default_spark_executor_memory,
    "default_spark_driver_memory": default_spark_driver_memory,
    "spark.default.numpartitions":spark_default_numpartitions,
    "spark.driver.port": spark_driver_port,
    "spark.driver.blockManager.port": spark_driver_block_manager_port,
    "spark.driver.host": spark_driver_host,
    "spark.driver.bindAddress": spark_driver_bind_address,
    "spark.dwapicentral.url": spark_dwapicentral_url,
    "spark.dwapicentral.driver": spark_dwapicentral_driver,
    "spark.dwapicentral.user": spark_dwapicentral_user,
    "spark.dwapicentral.password": spark_dwapicentral_password,   
    "spark.ods.numpartitions":spark_ods_numpartitions,  
    "spark.ods.url": spark_ods_url,
    "spark.ods.driver": spark_ods_driver,
    "spark.ods.user": spark_ods_user,
    "spark.ods.password": spark_ods_password,
    "spark.edw.url": spark_edw_url,
    "spark.edw.driver": spark_edw_driver,
    "spark.edw.user": spark_edw_user,
    "spark.edw.password": spark_edw_password,
    "spark.dimAgency.dbtable": spark_dim_agency_table,
    "spark.dimARTOutcome.dbtable": spark_dim_art_outcome_table,
    "spark.dimAgeGroup.dbtable": spark_dim_age_group_table,
    "spark.dimDate.dbtable": spark_dim_date_table,
    "spark.dimFacility.dbtable": spark_dim_facility_table,
    "spark.dimPartner.dbtable": spark_dim_partner_table,
    "spark.dimPatient.dbtable": spark_dim_patient_table,
    "spark.dimDifferentiatedCare.dbtable":spark_dim_differentiated_care_table,
    "spark.dimRegimenLine.dbtable": spark_dim_regimen_line_table,
    "spark.dimRelationshipWithPatient.dbtable": spark_dim_relationship_with_patient_table,
    "spark.dimDrug.dbtable": spark_dim_drug_table,
    "spark.historicalArtOutcomeBase.dbtable": spark_historical_art_outcome_base_table,
    "spark.factArtHistory.dbtable": spark_fact_art_history_table,
    "spark.factArt.dbtable": spark_fact_art_table,
    "spark.factOtz.dbtable":spark_fact_otz_table,
    "spark.factOvc.dbtable": spark_fact_ovc_table,
    "spark.factLatestObs.dbtable": spark_fact_latest_obs_table,
    "spark.factCovid.dbtable": spark_fact_covid_table,
    "spark.intermediateArtOutcomes.dbtable": spark_intermediate_art_outcomes_table,
    "spark.sql.autoBroadcastJoinThreshold":-1
}

default_args = {
    'owner': 'kenyahmis',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 25, tzinfo=local_tz),
    'email': ['paul.nthusi@thepalladiumgroup.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}
dag = DAG(dag_id='edw_dims_etl_dag',
          default_args=default_args,
          catchup=False,
          schedule_interval=None)

load_dim_age_group = build_age_group_dimension(dag = dag, default_conf = default_conf)
load_dim_agency = build_load_dim_agency(dag = dag, default_conf = default_conf)
load_dim_date = build_load_date_dimension(dag = dag, default_conf = default_conf)
load_dim_differentiated_care = build_differentiated_care_dimension(dag = dag, default_conf = default_conf)
load_drug_dimension = build_drug_dimension(dag = dag, default_conf = default_conf)
load_dim_facilities = build_load_dim_facilities(dag = dag, default_conf = default_conf)
load_dim_hts_trace_outcome = build_load_dim_hts_trace_outcome(dag = dag, default_conf = default_conf)
load_dim_hts_trace_type = build_load_dim_hts_trace_type(dag = dag, default_conf = default_conf)
load_dim_partners = build_load_dim_partners(dag = dag, default_conf = default_conf)
load_dim_patients = build_load_dim_patients(dag = dag, default_conf = default_conf)
load_dim_regimen_line = build_regimen_line_dimension(dag = dag, default_conf = default_conf)
load_dim_relation_to_patient = build_relation_to_patient_dimension(dag = dag, default_conf = default_conf)
load_dim_test_kit_name = build_load_dim_test_kit_name(dag = dag, default_conf = default_conf)
load_dim_treatment_type = build_load_dim_treatment_type(dag = dag, default_conf = default_conf)

load_dim_age_group >> load_dim_agency >> load_dim_date >> load_dim_differentiated_care
load_dim_differentiated_care >> load_drug_dimension >> load_dim_facilities >> load_dim_hts_trace_outcome
load_dim_hts_trace_outcome >> load_dim_hts_trace_type >> load_dim_partners >> load_dim_patients
load_dim_patients >> load_dim_regimen_line >> load_dim_relation_to_patient >> load_dim_test_kit_name
load_dim_test_kit_name >>  load_dim_treatment_type
