import json
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector.pandas_tools import pd_writer
from snowflake.connector import connect
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine


def read_json_data(path):
    """Transform a json file into a python dictionary

    :param path: path where file is located
    :type path: str
    :return: a python dictionary needed to create a pandas dataframe
    :rtype: dict
    """
    with open(path, "r") as f:
        missed_days_dict = json.load(f)

    return missed_days_dict


def create_dataframe(task, key_to_normalize, ti):
    """Create a pandas dataframe from a key

    :param task: the task that contains da data
    :type task: dict
    :param key_to_normalize: key that contains the records
    :type key_to_normalize: list
    :param ti: task intance
    :return: a pandas dataframe with snakecase column names
    :rtype: pd.DataFrame
    """
    xcom_pull_obj = ti.xcom_pull(task_ids=task)
    df = pd.json_normalize(xcom_pull_obj[key_to_normalize], sep="_")

    return df


def transform(task, how_to_merge, pk_fk, ti):
    """Joins two dataframes based on one column

    :param task: _description_
    :type task: _type_
    :param ti: _description_
    :type ti: _type_
    :return: _description_
    :rtype: _type_
    """
    df_student = ti.xcom_pull(task_ids=task)[0]
    df_missed_days = ti.xcom_pull(task_ids=task)[1]

    students_grades_and_missed_days = df_student.merge(
        df_missed_days, how=how_to_merge, on=pk_fk
    )

    # cast data
    df_casted = students_grades_and_missed_days.astype(
        {
            "student_id": str,
            "name": str,
            "grades_math": "Int64",
            "grades_science": "Int64",
            "grades_history": "Int64",
            "grades_english": "Int64",
        }
    )

    # all columns in upper case so that snowflake accepts it
    df_casted.columns = [item.upper() for item in df_casted.columns]

    return df_casted


def send_to_snowflake(
    user, password, account, warehouse, database, schema, role, task, ti
):
    """Send the one big table to snowflake"""

    # get the OBT
    df_student_snflk = ti.xcom_pull(task_ids=task)[0]

    # Create table schema
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
    # create snoeflake connections
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

    # send data to snowflake
    table_name = "student_data"
    with engine.connect() as con:
        con.execute(sql)

        df_student_snflk.to_sql(
            name=table_name,
            con=con,
            if_exists="replace",
            index=False,
            method=pd_writer,
        )

        con.close()
