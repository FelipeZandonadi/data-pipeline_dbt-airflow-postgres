from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, ExecutionConfig, RenderConfig

import os
from datetime import datetime, timedelta

DBT_PROJECT_PATH = f"{os.environ["AIRFLOW_HOME"]}/dags/dbt/"
DBT_EXECUTABLE_PATH = f"{os.environ["AIRFLOW_HOME"]}/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
	profile_name="lakehouse",
	target_name="dev",
 
	profile_mapping=PostgresUserPasswordProfileMapping(
    	conn_id="postgres_conn",
    	profile_args={
			"database":"lakehouse",
            "schema":"brz_data",
            },
	),
)

execution_config = ExecutionConfig(
	dbt_executable_path=DBT_EXECUTABLE_PATH
)

default_args = {
	"owner":"airflow",
	"start_date":datetime(2024,10,24),
	"email_on_failure":False,
	"email_on_retry":False,
	"retries":1,
	"retry_delay":timedelta(seconds=15)
}

with DAG(
	dag_id="dag_brz",
	default_args=default_args,
	description="Executa camada brz para sales",
	schedule_interval=None,
	catchup=False,
	tags=["brz","sales"]
):
	start = EmptyOperator(task_id = "Start")

	dbt_run_and_test = DbtTaskGroup(
    	group_id="execute_sales_brz",
    	project_config=ProjectConfig(
        	DBT_PROJECT_PATH,
        	models_relative_path="models"
    	),
    	profile_config=profile_config,
		execution_config=execution_config,
    	render_config=RenderConfig(select=["path:models/brz_data/"]),
    	operator_args={"install_deps": True},
    	default_args={"retries": 1,
                  	"retry_delay": timedelta(seconds=30)}
	)

	end = EmptyOperator(task_id = "End")

	start >> dbt_run_and_test >> end