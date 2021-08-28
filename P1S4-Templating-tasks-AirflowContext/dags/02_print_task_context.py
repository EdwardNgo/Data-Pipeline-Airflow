import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="chapter4_print_context",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@daily",
)


def _print_context(**kwargs):  # thay kwargs thanh context cung duoc  
    print(kwargs)


print_context = PythonOperator(
    task_id="print_context",
    python_callable=_print_context,
    dag=dag,
    )