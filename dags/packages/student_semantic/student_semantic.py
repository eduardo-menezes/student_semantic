import json
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector.pandas_tools import pd_writer
from snowflake.connector import connect
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine


def read_json_data(path):
    with open(path, "r") as f:
        missed_days_dict = json.load(f)

    return missed_days_dict


def create_dataframe(task, key_to_normalize, ti):
    xcom_pull_obj = ti.xcom_pull(task_ids=task)
    df = pd.json_normalize(xcom_pull_obj[key_to_normalize], sep="_")

    return df


def transform(task, ti):
    df_student = ti.xcom_pull(task_ids=task)[0]
    df_missed_days = ti.xcom_pull(task_ids=task)[1]

    students_grades_and_missed_days = df_student.merge(
        df_missed_days, how="left", on="student_id"
    )
    return students_grades_and_missed_days


def send_to_snowflake(
    user, password, account, warehouse, database, schema, role, task, ti
):
    df_student = ti.xcom_pull(task_ids=task)[0]
    df_casted = df_student.astype(
        {
            "student_id": str,
            "name": str,
            "grades_math": "Int64",
            "grades_science": "Int64",
            "grades_history": "Int64",
            "grades_english": "Int64",
        }
    )
    df_casted.columns = [item.upper() for item in df_casted.columns]

    sql = """
        CREATE OR REPLACE TABLE student_data(
        student_id VARCHAR, 
        name VARCHAR, 
        grades_math INTEGER,
        grades_science INTEGER,
        grades_history INTEGER,
        grades_english INTEGER,
        missed_days INTEGER);

        """

    engine = create_engine(
        URL(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role,
            insecure_mode=True,
        )
    )

    table_name = "student_data"
    with engine.connect() as con:
        con.execute(sql)

        df_casted.to_sql(
            name=table_name,
            con=con,
            if_exists="replace",
            index=False,
            method=pd_writer,
        )

        con.close()
