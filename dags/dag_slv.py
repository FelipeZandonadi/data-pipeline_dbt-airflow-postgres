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
            "schema":"slv_data"}
	)
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

def create_dbt_run_and_test(models, gp_id):
		
		return DbtTaskGroup(
            group_id=f"execute_sales_slv_{gp_id}",
            project_config=ProjectConfig(
                DBT_PROJECT_PATH,
                models_relative_path="models"
            ),
            profile_config=profile_config,
            execution_config=execution_config,
            render_config=RenderConfig(
				select=[f"path:models/slv_data/{model}.sql" for model in models]
			),
            operator_args={"install_deps": True},
            default_args={"retries": 0}
	)

with DAG(
	dag_id="dag_slv",
	default_args=default_args,
	description="Executa camada slv para sales",
	schedule_interval=None,
	catchup=False,
	tags=["slv","sales"]
):
	
	start = EmptyOperator(task_id = "Start")
	
	execute_sale_slv_1 = create_dbt_run_and_test(['slv_tb_customers', 'slv_tb_products', 'slv_tb_sellers'], 1)
	execute_sale_slv_2 = create_dbt_run_and_test(['slv_tb_orders'], 2)
	execute_sale_slv_3 = create_dbt_run_and_test(['slv_tb_order_items', 'slv_tb_order_payments', 'slv_tb_order_reviews'], 3)

	end = EmptyOperator(task_id = "End")
	
	start >> execute_sale_slv_1 >> execute_sale_slv_2 >> execute_sale_slv_3 >> end