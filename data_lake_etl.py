from datetime import datetime, timedelta
from random import randint

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator

OWNER = 'gosipenkov'

def hive_task(owner, dag, task_id, query):
    return DataProcHiveOperator(
        dag=dag,
        task_id=task_id,

        cluster_name='cluster-dataproc',
        region='europe-west3',
        job_name='_'.join((owner, task_id,
            '{{ execution_date.year }}', '{{ params.job_suffix }}')),

        query=query,

        params={'job_suffix': randint(0, 100000)}
    )


default_args = {
    'owner': OWNER,
    'start_date': datetime(2013, 1, 1)
}

dag = DAG(
    '_'.join((OWNER, 'data_lake_etl')),
    default_args=default_args,
    schedule_interval='0 0 1 1 *'
)

ods_billing = hive_task(OWNER, dag, 'ods_billing', """
    INSERT OVERWRITE TABLE gosipenkov.ods_billing
    PARTITION (year = {{ execution_date.year }})
    SELECT
        user_id,
        CAST(SUBSTR(billing_period, 1, 4) || SUBSTR(billing_period, 6, 2) AS INT) AS billing_period,
        service,
        tariff,
        CAST(`sum` AS DECIMAL) AS amount,
        CAST(created_at AS DATE) AS created_at
    FROM gosipenkov.stg_billing
    WHERE YEAR(created_at) = {{ execution_date.year }};
""")

ods_issue = hive_task(OWNER, dag, 'ods_issue', """
    INSERT OVERWRITE TABLE gosipenkov.ods_issue
    PARTITION (year = {{ execution_date.year }})
    SELECT
        CAST(user_id AS INT) AS user_id,
        CAST(CAST(start_time AS TIMESTAMP) AS DATE) AS start_date,
        CAST(CAST(end_time AS TIMESTAMP) AS DATE) AS end_date,
        title,
        description,
        service
    FROM gosipenkov.stg_issue
    WHERE YEAR(end_time) = {{ execution_date.year }};
""")

ods_payment = hive_task(OWNER, dag, 'ods_payment', """
    INSERT OVERWRITE TABLE gosipenkov.ods_payment
    PARTITION (year = {{ execution_date.year }})
    SELECT
        user_id,
        pay_doc_type AS doc_type,
        pay_doc_num AS doc_num,
        account,
        phone,
        CAST(SUBSTR(billing_period, 1, 4) || SUBSTR(billing_period, 6, 2) AS INT) AS billing_period,
        CAST(pay_date AS DATE) AS pay_date,
        CAST(`sum` AS DECIMAL) AS amount
    FROM gosipenkov.stg_payment
    WHERE YEAR(pay_date) = {{ execution_date.year }};
""")

ods_traffic = hive_task(OWNER, dag, 'ods_traffic', """
    INSERT OVERWRITE TABLE gosipenkov.ods_traffic
    PARTITION (year = {{ execution_date.year }})
    SELECT
        user_id,
        CAST((CAST(`timestamp` AS BIGINT) DIV 1000) AS INT) AS unixtime,
        device_id,
        device_ip_addr,
        bytes_sent,
        bytes_received
    FROM gosipenkov.stg_traffic
    WHERE YEAR(FROM_UNIXTIME(CAST(`timestamp` AS BIGINT) DIV 1000)) = {{ execution_date.year }};
""")

dm_traffic_stats = hive_task(OWNER, dag, 'dm_traffic_stats', """
    INSERT OVERWRITE TABLE gosipenkov.dm_traffic_stats
    PARTITION (year = {{ execution_date.year }})
    SELECT
        user_id,
        MIN(bytes_received) AS bytes_received_min,
        AVG(bytes_received) AS bytes_received_avg,
        MAX(bytes_received) AS bytes_received_max
    FROM gosipenkov.ods_traffic
    WHERE year = {{ execution_date.year }}
    GROUP BY user_id;
""")

ods_traffic >> dm_traffic_stats
