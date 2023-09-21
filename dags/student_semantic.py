from airflow.models import DAG, Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from packages.student_semantic.student_semantic import (
    read_json_data,
    create_dataframe,
    transform,
    send_to_snowflake,
)
import datetime as dt

user = Variable.get("user")
password = Variable.get("password")
account = Variable.get("account")
warehouse = Variable.get("warehouse")
database = Variable.get("database")
schema = Variable.get("schema")
role = Variable.get("role")
path_student_data = "data/raw/students/students.json"
path_missed_days_data = "data/raw/missed_days/missed_days.json"

with DAG(
    "student_semantic",
    schedule="0 5 * * *",
    start_date=dt.datetime(2023, 9, 20),
    catchup=False,
) as dag:
    start_pipeline = DummyOperator(task_id="start")

    read_student_data = PythonOperator(
        task_id="read_student_data",
        python_callable=read_json_data,
        op_kwargs={"path": path_student_data},
        provide_context=True,
    )

    read_missed_days_data = PythonOperator(
        task_id="read_missed_days_data",
        python_callable=read_json_data,
        op_kwargs={"path": path_missed_days_data},
        provide_context=True,
    )

    create_dataframe_student = PythonOperator(
        task_id="create_dataframe_student",
        python_callable=create_dataframe,
        op_kwargs={
            "task": "read_student_data",
            "key_to_normalize": "students",
        },
        provide_context=True,
    )

    create_dataframe_missed_days = PythonOperator(
        task_id="create_dataframe_missed_days",
        python_callable=create_dataframe,
        op_kwargs={
            "task": "read_missed_days_data",
            "key_to_normalize": "missed_classes",
        },
        provide_context=True,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform,
        op_kwargs={
            "task": ["create_dataframe_student", "create_dataframe_missed_days"]
        },
        provide_context=True,
    )

    send_to_snowflake = PythonOperator(
        task_id="send_to_snowflake",
        python_callable=send_to_snowflake,
        op_kwargs={
            "task": ["transform"],
            "user": user,
            "password": password,
            "account": account,
            "warehouse": warehouse,
            "database": database,
            "schema": schema,
            "role": role,
        },
        provide_context=True,
    )

    start_pipeline >> [read_student_data, read_missed_days_data],
    read_student_data >> create_dataframe_student,
    read_missed_days_data >> create_dataframe_missed_days,
    [create_dataframe_student, create_dataframe_missed_days] >> transform,
    transform >> send_to_snowflake
