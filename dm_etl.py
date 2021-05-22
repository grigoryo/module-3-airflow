from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator


def pg_task(owner, dag, task_id, query):
    return PostgresOperator(
        dag=dag,
        task_id=task_id,

        sql=query
    )


def tie_task(owner, dag, task_id):
    return DummyOperator(
        dag=dag,
        task_id=task_id
    )


OWNER = 'gosipenkov'

default_args = {
    'owner': OWNER,
    'depends_on_past': True
}


dag = DAG(
    '_'.join((OWNER, 'dwh_etl')),
    default_args=default_args,
    schedule_interval='None'
)


load_rep_payment_tmp = pg_task(OWNER, dag, 'load_rep_payment_tmp', """
    SELECT gosipenkov.load_rep_payment_tmp();
""")

tmps_ok = tie_task(OWNER, dag, 'tmps_ok')

truncate_rep_payment_fact = pg_task(OWNER, dag, 'truncate_rep_payment_fact', """
    SELECT gosipenkov.truncate_rep_payment_fact();
""")

truncates_ok = tie_task(OWNER, dag, 'truncates_ok')

load_rep_payment_dim_billing_mode = pg_task(OWNER, dag, 'load_rep_payment_dim_billing_mode', """
    SELECT gosipenkov.load_rep_payment_dim_billing_mode();
""")

load_rep_payment_dim_billing_year = pg_task(OWNER, dag, 'load_rep_payment_dim_billing_year', """
    SELECT gosipenkov.load_rep_payment_dim_billing_year();
""")

load_rep_payment_dim_district = pg_task(OWNER, dag, 'load_rep_payment_dim_district', """
    SELECT gosipenkov.load_rep_payment_dim_district();
""")

load_rep_payment_dim_legal_type = pg_task(OWNER, dag, 'load_rep_payment_dim_legal_type', """
    SELECT gosipenkov.load_rep_payment_dim_legal_type();
""")

load_rep_payment_dim_user_reg_year = pg_task(OWNER, dag, 'load_rep_payment_dim_user_reg_year', """
    SELECT gosipenkov.load_rep_payment_dim_user_reg_year();
""")

dims_ok = tie_task(OWNER, dag, 'dims_ok')

load_rep_payment_fact = pg_task(OWNER, dag, 'load_rep_payment_fact', """
    SELECT gosipenkov.load_rep_payment_fact();
""")

facts_ok = tie_task(OWNER, dag, 'facts_ok')


load_rep_payment_tmp >> tmps_ok

tmps_ok >> truncate_rep_payment_fact >> truncates_ok

truncates_ok >> load_rep_payment_dim_billing_mode >> dims_ok
truncates_ok >> load_rep_payment_dim_billing_year >> dims_ok
truncates_ok >> load_rep_payment_dim_district >> dims_ok
truncates_ok >> load_rep_payment_dim_legal_type >> dims_ok
truncates_ok >> load_rep_payment_dim_user_reg_year >> dims_ok

dims_ok >> load_rep_payment_fact >> facts_ok
