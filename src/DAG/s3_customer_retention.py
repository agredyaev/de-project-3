"""
Dag designed to generate customer retention report\n
\n
1. request API of report generation\n
2. push given report id to xcom\n
3. push given increment id to xcom\n
4. create staging mart tables\n
5. removes data for the given date\n
6. insert data to staging tables\n
7. update data in the mart tables\n
"""


import os
from datetime import datetime, timedelta

from click import option
from utils.customer_retention import *


from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

DAG_ID = os.path.basename(__file__).split('.')[0]

start_date = get_date(8)

with DAG(
        DAG_ID,
        default_args=DEFAULT_ARGS,
        catchup=True,
        start_date=start_date,
        end_date=get_date(1),
) as dag:
    dag.doc_md = __doc__

    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report
    )

    get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report
    )

    get_increment = PythonOperator(
        task_id='get_increment',
        python_callable=get_increment,
        op_kwargs={'date': BUSINESS_DT}
    )

    remove_duplicates = PostgresOperator(
        task_id='remove_duplicates',
        postgres_conn_id=CONNECTION_ID,
        sql="sql/remove_duplicates.sql",
        parameters={"date": {BUSINESS_DT}},
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    for tablename in STAGE:

        create_stage = f'create_staging_{tablename}'
        path = f'{DIR}create_staging.{tablename}.sql'

        globals()[create_stage] = PythonOperator(
            task_id=create_stage,
            python_callable=run_slq_query,
            op_kwargs={'path': path}
        )

    for tablename in MART:

        create_mart = f'create_mart_{tablename}'
        path = f'{DIR}create_mart.{tablename}.sql'

        globals()[create_mart] = PythonOperator(
            task_id=create_mart,
            python_callable=run_slq_query,
            op_kwargs={'path': path}
        )

    for tablename, filename in STAGE.items():
        update_stage = f'update_staging_{tablename}_cmn'

        globals()[update_stage] = PythonOperator(
            task_id=update_stage,
            python_callable=upload_data,
            op_kwargs={'xcom_key': 'report_id',
                       'filename': f'{filename}.csv',
                       'date': BUSINESS_DT,
                       'pg_table': tablename,
                       'pg_schema': 'staging'}
        )

    for tablename, filename in STAGE.items():
        update_stage_inc = f'update_staging_{tablename}_incrx'

        globals()[update_stage_inc] = PythonOperator(
            task_id=update_stage_inc,
            python_callable=upload_data,
            op_kwargs={'xcom_key': 'increment_id',
                       'filename': f'{filename}_inc.csv',
                       'date': BUSINESS_DT,
                       'pg_table': tablename,
                       'pg_schema': 'staging'}
        )

    for tablename in MART:
        if 'f_' not in tablename:
            update_mart = f'update_mart_{tablename}'
            path = f'{DIR}update_mart.{tablename}.sql'

            globals()[update_mart] = PythonOperator(
                task_id=update_mart,
                python_callable=run_slq_query,
                op_kwargs={'path': path}
            )

    update_mart_f_sales = PostgresOperator(
        task_id='update_mart_f_sales',
        postgres_conn_id=CONNECTION_ID,
        sql="sql/update_mart.f_sales.sql",
        parameters={"date": {BUSINESS_DT}},
        trigger_rule=TriggerRule.ALL_SUCCESS

    )


    update_mart_f_customer_retention = PostgresOperator(
        task_id='update_mart_f_customer_retention',
        postgres_conn_id=CONNECTION_ID,
        sql="sql/update_mart.f_customer_retention.sql",
        trigger_rule=TriggerRule.ALL_SUCCESS

    )

    def condition(date: str, **context) -> None:
        date = datetime.strptime(date, '%Y-%m-%d')

        if date > start_date:
            return 'update_increment'
        else:
            return 'update_common'

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=condition,
        op_kwargs={'date': BUSINESS_DT},
        trigger_rule=TriggerRule.ALL_SUCCESS

    )

    options = ['update_common', 'update_increment',
               'branch_a_dummy1', 'branch_a_dummy2', 'branch_a_dummy3', 'branch_b_dummy1']

    for branch in options:
        globals()[branch] = EmptyOperator(
            task_id=branch,
            trigger_rule=TriggerRule.ALL_SUCCESS
        )

    branching_follow = EmptyOperator(
        task_id='branching_follow',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    dict_ = globals().copy()
    create_stagings = get_vars(dict_, 'create_staging_')
    update_stagings = get_vars(dict_, '_cmn')
    update_stagings_inc = get_vars(dict_, '_incrx')
    create_marts = get_vars(dict_, 'create_mart_')
    update_marts = get_vars(dict_, 'update_mart_', 'f_')

    generate_report >> get_report >> get_increment >> branching

    (branching >> update_common >> create_stagings >> branch_a_dummy1 >>
     update_stagings >> branch_a_dummy2 >> create_marts >> branch_a_dummy3 >> branching_follow)

    (branching >> update_increment >> remove_duplicates >> update_stagings_inc >>
     branch_b_dummy1 >> branching_follow)

    branching_follow >> update_marts >> update_mart_f_sales >> update_mart_f_customer_retention
