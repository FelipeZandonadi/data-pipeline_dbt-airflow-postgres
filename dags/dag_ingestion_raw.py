from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from airflow.models import DAG
from datetime import datetime
from airflow.operators.empty import EmptyOperator
import pandas as pd

FILE_PATH_S3 = "s3://olist-data-raw-trainee/"
CONN_ID_S3 = "aws"
CONN_ID_POSTGRES = 'postgres_conn'

with DAG(dag_id='ingestion_S3_to_postgres', description="Ingestion data raw from bucket S3 to DB postgres",
        schedule_interval=None, start_date=datetime(2025,3,19),
        catchup=False) as dag:
    
    start = EmptyOperator(task_id="Start")

    orders_to_postgres = aql.load_file(
        task_id="orders",
        input_file=File(path=FILE_PATH_S3 + "orders.csv", conn_id=CONN_ID_S3),
        output_table=Table(
            name="tb_orders",
            conn_id="postgres_conn",
            metadata=Metadata(schema="raw", 
                              database="lakehouse",),
        )
    )
    
    customers_to_postgres = aql.load_file(
        task_id="customers",
        input_file=File(path=FILE_PATH_S3 + "customers.csv", conn_id=CONN_ID_S3),
        output_table=Table(
            name="tb_customers",
            conn_id="postgres_conn",
            metadata=Metadata(schema="raw",
                              database="lakehouse",),
            )
    )
    
    order_payments_to_postgres = aql.load_file(
        task_id="order_payments",
        input_file=File(path=FILE_PATH_S3 + "order_payments.csv", conn_id=CONN_ID_S3),
        output_table=Table(
            name="tb_order_payments",
            conn_id="postgres_conn",
            metadata=Metadata(schema="raw",
                              database="lakehouse",),
        )
    )
    
    order_items_to_postgres = aql.load_file(
        task_id="order_items",
        input_file=File(path=FILE_PATH_S3 + "order_items.csv", conn_id=CONN_ID_S3),
        output_table=Table(
            name="tb_order_items",
            conn_id="postgres_conn",
            metadata=Metadata(schema="raw",
                              database="lakehouse",),
        )
    )

    products_to_postgres = aql.load_file(
        task_id="products",
        input_file=File(path=FILE_PATH_S3 + "products.csv", conn_id=CONN_ID_S3),
        output_table=Table(
            name="tb_products",
            conn_id="postgres_conn",
            metadata=Metadata(schema="raw",
                              database="lakehouse",),
        )
    )

    end = EmptyOperator(task_id="End")
    
    # trigger_dag = TriggerDagRunOperator(
    #     task_id="trigger_dag_dbt_brz",
    #     trigger_dag_id="brz_dag", 
    # )
    
    start >> [orders_to_postgres, customers_to_postgres, order_payments_to_postgres, order_items_to_postgres, products_to_postgres] >> end