from datetime import datetime, timedelta
from airflow.models import Variable
import pendulum
from airflow import DAG
from includes.load_dimensions_task import build_load_dimensions_task
from includes.load_date_dimension_task import build_load_date_dimension
from includes.load_art_history_fact_task import build_art_history_fact

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

spark_dwapicentral_url = Variable.get("SPARK_DWAPICENTRAL_URL")
spark_dwapicentral_driver = Variable.get("SPARK_DWAPICENTRAL_DRIVER")
spark_dwapicentral_user = Variable.get("SPARK_DWAPICENTRAL_USER")
spark_dwapicentral_password = Variable.get("SPARK_DWAPICENTRAL_PASSWORD")
spark_ods_url = Variable.get("SPARK_ODS_URL")
spark_ods_driver = Variable.get("SPARK_ODS_DRIVER")
spark_ods_user = Variable.get("SPARK_ODS_USER")
spark_ods_password = Variable.get("SPARK_ODS_PASSWORD")
spark_edw_url = Variable.get("SPARK_EDW_URL")
spark_edw_driver = Variable.get("SPARK_EDW_DRIVER")
spark_edw_user = Variable.get("SPARK_EDW_USER")
spark_edw_password = Variable.get("SPARK_EDW_PASSWORD")
spark_dim_agency_table = Variable.get("SPARK_DIM_AGENCY_TABLE")
spark_dim_art_outcome_table = Variable.get("SPARK_DIM_ART_OUTCOME_TABLE")
spark_dim_date_table = Variable.get("SPARK_DIM_DATE_TABLE")
spark_dim_facility_table = Variable.get("SPARK_DIM_FACILITY_TABLE")
spark_dim_partner_table = Variable.get("SPARK_DIM_PARTNER_TABLE")
spark_dim_patient_table = Variable.get("SPARK_DIM_PATIENT_TABLE")
spark_historical_art_outcome_base_table = Variable.get("SPARK_HISTORICAL_ART_OUTCOME_BASE_TABLE")
spark_fact_art_history_table = Variable.get("SPARK_FACT_ART_HISTORY_TABLE")

default_conf = {
    "connection_id": "spark_standalone",
    "spark_app_home":spark_app_home,
    "default_spark_total_executor_cores": default_spark_total_executor_cores,
    "default_spark_executor_cores": default_spark_executor_cores,
    "default_spark_executor_memory": default_spark_executor_memory,
    "default_spark_driver_memory": default_spark_driver_memory,
    "spark.driver.port": spark_driver_port,
    "spark.driver.blockManager.port": spark_driver_block_manager_port,
    "spark.driver.host": spark_driver_host,
    "spark.driver.bindAddress": spark_driver_bind_address,
    "spark.dwapicentral.url": spark_dwapicentral_url,
    "spark.dwapicentral.driver": spark_dwapicentral_driver,
    "spark.dwapicentral.user": spark_dwapicentral_user,
    "spark.dwapicentral.password": spark_dwapicentral_password,    
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
    "spark.dimDate.dbtable": spark_dim_date_table,
    "spark.dimFacility.dbtable": spark_dim_facility_table,
    "spark.dimPartner.dbtable": spark_dim_partner_table,
    "spark.dimPatient.dbtable": spark_dim_patient_table,
    "spark.historicalArtOutcomeBase.dbtable": spark_historical_art_outcome_base_table,
    "spark.factArtHistory.dbtable": spark_fact_art_history_table,
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
dag = DAG(dag_id='edw_etl_dag',
          default_args=default_args,
          catchup=False,
          schedule_interval="0 14 15 * *")

load_dimensions = build_load_dimensions_task(dag = dag, default_conf = default_conf)
# load_date_dimension = build_load_date_dimension(dag = dag, default_conf = default_conf)
art_history = build_art_history_fact(dag = dag, default_conf = default_conf)

# load_date_dimension >>
load_dimensions >> art_history