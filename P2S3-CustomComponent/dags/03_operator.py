import datetime as dt

from airflow import DAG

from custom.operator import MovielensFetchRatingsOperator


with DAG(
    dag_id="03_operator",
    description="Fetches ratings from the Movielens API using a custom operator.",
    start_date=dt.datetime(2021, 8, 25),
    end_date=dt.datetime(2021, 8, 31),
    schedule_interval="@daily",
) as dag:
    MovielensFetchRatingsOperator(
        task_id="fetch_ratings",
        conn_id="movielens",
        start_date="{{ds}}",
        end_date="{{next_ds}}",
        output_path="/data/custom_operator/{{ds}}.json",
    )