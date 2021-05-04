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
    'depends_on_past': True,
    'start_date': datetime(2013, 1, 1)
}


dag = DAG(
    '_'.join((OWNER, 'dwh_etl')),
    default_args=default_args,
    schedule_interval='0 0 1 1 *'
)


load_ods_billing = pg_task(OWNER, dag, 'load_ods_billing', """
    SELECT gosipenkov.load_ods_billing({{ execution_date.year }});
""")

load_ods_issue = pg_task(OWNER, dag, 'load_ods_issue', """
    SELECT gosipenkov.load_ods_issue({{ execution_date.year }});
""")

load_ods_payment = pg_task(OWNER, dag, 'load_ods_payment', """
    SELECT gosipenkov.load_ods_payment({{ execution_date.year }});
""")

load_ods_traffic = pg_task(OWNER, dag, 'load_ods_traffic', """
    SELECT gosipenkov.load_ods_traffic({{ execution_date.year }});
""")

ods_ok = tie_task(OWNER, dag, 'ods_ok')

load_hub_pay_doc = pg_task(OWNER, dag, 'load_hub_pay_doc', """
    SELECT gosipenkov.load_hub_pay_doc({{ execution_date.year }});
""")

load_hub_user = pg_task(OWNER, dag, 'load_hub_user', """
    SELECT gosipenkov.load_hub_user({{ execution_date.year }});
""")

hubs_ok = tie_task(OWNER, dag, 'hubs_ok')

load_lnk_payment = pg_task(OWNER, dag, 'load_lnk_payment', """
    SELECT gosipenkov.load_lnk_payment({{ execution_date.year }});
""")

links_ok = tie_task(OWNER, dag, 'links_ok')

load_sat_pay_doc_detail = pg_task(OWNER, dag, 'load_sat_pay_doc_detail', """
    SELECT gosipenkov.load_sat_pay_doc_detail({{ execution_date.year }});
""")

load_sat_payment_billing_period = pg_task(OWNER, dag, 'load_sat_payment_billing_period', """
    SELECT gosipenkov.load_sat_payment_billing_period({{ execution_date.year }});
""")

load_sat_user_detail = pg_task(OWNER, dag, 'load_sat_user_detail', """
    SELECT gosipenkov.load_sat_user_detail({{ execution_date.year }});
""")

sats_ok = tie_task(OWNER, dag, 'sats_ok')


load_ods_billing >> ods_ok
load_ods_issue >> ods_ok
load_ods_payment >> ods_ok
load_ods_traffic >> ods_ok

ods_ok >> load_hub_pay_doc >> hubs_ok
ods_ok >> load_hub_user >> hubs_ok

hubs_ok >> load_lnk_payment >> links_ok

links_ok >> load_sat_pay_doc_detail >> sats_ok
links_ok >> load_sat_payment_billing_period >> sats_ok
links_ok >> load_sat_user_detail >> sats_ok
