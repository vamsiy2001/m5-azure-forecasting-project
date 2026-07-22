from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id="m5_pipeline",
    description="M5 forecasting: bronze -> silver -> gold -> training, orchestrated end to end",
    schedule=None,          # no automatic schedule yet -- we'll trigger it manually while building
    start_date=datetime(2026, 1, 1),
    catchup=False,          # don't backfill runs for every day since start_date
    tags=["m5", "forecasting"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_bronze_data = BashOperator(
        task_id="check_bronze_data",
        bash_command='echo "Placeholder: will verify today'"'"'s bronze files landed correctly."',
    )

    # TODO (Stage 5): replace with a real task that triggers the Databricks
    # bronze -> silver notebook job once that stage is built.
    transform_silver = EmptyOperator(task_id="transform_silver_databricks")

    # TODO (Stage 6/7): replace with tasks triggering the Synapse Spark
    # gold-aggregation job and `dbt run` once those stages are built.
    aggregate_gold = EmptyOperator(task_id="aggregate_gold")

    # TODO (Stage 7): replace with the LightGBM/MLflow training job.
    train_model = EmptyOperator(task_id="train_model")

    end = EmptyOperator(task_id="end")

    start >> check_bronze_data >> transform_silver >> aggregate_gold >> train_model >> end
