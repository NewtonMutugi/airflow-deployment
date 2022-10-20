# airflow-deployment

This repository contains deployments for running a dockerized version of airflow. 
There are two deployment options (local & production). The local directory has a compose file with all the necessary services required for a first time run whereas the production directory only has the core airflow components and assumes you have your own installation of Postgresql(airflow's internal database)

## Requirements
- Docker
- PostgreSQL (When running from the production directory)

## Running airflow
1. It's possible to run the airflow containers using the official images but you should consider builing a custom image if you intend to run the DWH spark applications. The Dockerfile can be found in the `dockerfiles` directory. Remember to use the defined image tag when running the local/production container. Below is an example of how to generate the image. 

```bash
cd dockerfiles
docker builed -t khmis/airflow:0.0.1 .
```
2. You can define environment varibles using the `.env` file which can be found in both the local and production directories.

3. The `dag code` folder contains the main dag file (`dwh_etl_dag.py`) and component task files. When airflow runs fo the first time, the dags,plugins,logs and sparkapps directories should be automatically generated. Copy the dag file to the dags directory, and the plugin files to the plugin directories. The sparkapps directory contains the spark applications used in the DWH ETL. You can download them [here](https://palladiumgroup-my.sharepoint.com/:f:/g/personal/paul_nthusi_thepalladiumgroup_com/EsBqa-kKN59BnFuSs0VuuMkB5L5FcQPtr-rnE9B6TCukLw?e=O6cAqk) and copy them to the directory

