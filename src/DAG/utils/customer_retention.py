
from ast import Dict
from datetime import date, datetime, timedelta
import time
from typing import Any, List, Dict
import json
import requests
import pandas as pd
from sqlalchemy import create_engine

N = 20

NICKNAME = 'agredyaev'
COHORT = '1'

DB_CONNECTION_STRING = 'postgresql+psycopg2://jovyan:jovyan@localhost/de'

BASE_URL = 'https://d5dg1j9kt695d30blp03.apigw.yandexcloud.net'
API_KEY = '5f55e6c0-e9e5-4a9c-b313-63c01fc31460'

HEADERS = {
    'X-Nickname': NICKNAME,
    'X-Cohort': COHORT,
    'X-Project': 'True',
    'X-API-KEY': API_KEY
    # 'Content-Type': 'application/x-www-form-urlencoded'
}

DEFAULT_ARGS = {
    "owner": NICKNAME,
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

BUSINESS_DT = '{{ ds }}'


STAGE = {
    'user_order_log': 'user_orders_log',
    'customer_research': 'customer_research',
    'user_activity_log': 'user_activity_log'
}


MART = ['d_city', 'd_customer', 'd_item', 'f_sales', 'f_customer_retention']
DIR = '/lessons/dags/sql/'
CONNECTION_ID = 'postgresql_de'

engine = create_engine(DB_CONNECTION_STRING)
conn = engine.connect()


def get_vars(dict_: Dict, temp: str, exept: str = '0') -> List:
    """Filters given dict with given string

    Args:
        dict_ (Dict): dict of variables
        temp (str): template

    Returns:
        List: result
    """
    return [var for key, var in dict_.items() if temp in key and exept not in key]


def get_date(days: int) -> datetime:
    """Calculates datetime from today with given time delta 

    Args:
        days (int): given number of days

    Returns:
        datetime: resulting datetime
    """
    return datetime.today() - timedelta(days=days)


def run_slq_query(path: str, ) -> None:
    """Executes sql query from a file

    Args:
        path (str): path to a file
    """
    with open(path) as file:
        query = file.read()
        conn.execute(query)


def generate_report(ti: Any) -> None:
    """Sends request to API for generating report

    Args:
        ti (Any): task instance
    """

    print('Making request generate_report')

    response = requests.post(f'{BASE_URL}/generate_report', headers=HEADERS)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    print(task_id)
    ti.xcom_push(key='task_id', value=task_id)

    print(f'Response is {response.content}')


def get_report(ti: Any) -> None:
    """Gets report id and pushes it to xcom

    Args:
        ti (Any): task instance

    Raises:
        TimeoutError: report id hasn't been obtained after 60 seconds
    """

    print('Making request get_report')

    task_id = ti.xcom_pull(key='task_id')

    print(task_id)

    report_id = None

    for _ in range(N):
        response = requests.get(
            f'{BASE_URL}/get_report?task_id={task_id}', headers=HEADERS)
        response.raise_for_status()

        print(f'Response is {response.content}')

        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(N)

    if not report_id:
        raise TimeoutError()

    ti.xcom_push(key='report_id', value=report_id)
    print(f'Report_id={report_id}')


def get_increment(date: date, ti: Any) -> None:
    """Gets increment id and pushes it to xcom

    Args:
        date (date): report's date
        ti (Any): task instance

    Raises:
        HTTPError: status code doesn't equal 200
    """

    print('Making request get_increment')

    report_id = ti.xcom_pull(key='report_id')
    response = requests.get(
        f'{BASE_URL}/get_increment?report_id={report_id}&date={str(date)}T00:00:00',
        headers=HEADERS)

    response.raise_for_status()

    print(f'Response is {response.content}')

    increment_id = json.loads(response.content)['data']['increment_id']
    ti.xcom_push(key='increment_id', value=increment_id)
    print(f'increment_id={increment_id}')


def upload_data(xcom_key: str, filename: str, date: date, pg_table: str, pg_schema: str, ti: Any) -> None:
    """Gets file and uploads data to a db table

    Args:
        xcom_key (str): key of a xcom oobject
        filename (str): name of the file
        date (date): date of the report
        pg_table (str): pg table
        pg_schema (str): pg schema
        ti (Any): task instance
    """

    xcom_id = ti.xcom_pull(key=xcom_key)
 
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{COHORT}/{NICKNAME}/project/{xcom_id}/{filename}'
   
    local_filename = '/lessons/' + date.replace('-', '') + '_' + filename

    response = requests.get(s3_filename)

    with open(f"{local_filename}", "wb") as file:
        file.write(response.content)

    df = pd.read_csv(local_filename)

    if 'id' in df.columns:
        df.drop_duplicates(subset=['id'])
        df.drop('id', axis=1, inplace=True)

    df.to_sql(pg_table, engine, schema=pg_schema,
              if_exists='append', index=False)
