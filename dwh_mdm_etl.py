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
    'start_date': datetime(2021, 5, 23)
}


dag = DAG(
    '_'.join((OWNER, 'dwh_mdm_etl')),
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)


mdm_load_stg_user = pg_task(OWNER, dag, 'mdm_load_stg_user', """
    SELECT gosipenkov.mdm_load_stg_user();
""")

mdm_load_ods_user = pg_task(OWNER, dag, 'mdm_load_ods_user', """
    SELECT gosipenkov.mdm_load_ods_user();
""")

mdm_load_hub_user = pg_task(OWNER, dag, 'mdm_load_hub_user', """
    SELECT gosipenkov.mdm_load_hub_user();
""")

mdm_load_sat_user_mdm = pg_task(OWNER, dag, 'mdm_load_sat_user_mdm', """
    SELECT gosipenkov.mdm_load_sat_user_mdm();
""")


mdm_load_stg_user >> mdm_load_ods_user >> mdm_load_hub_user >> mdm_load_sat_user_mdm
