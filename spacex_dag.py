from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2018, 1, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("spacex", default_args=default_args, schedule_interval="0 0 1 1 *")

rocket_types = {"all", "falcon1", "falcon9", "falconheavy"}

for rocket_type in rocket_types:
    t1 = BashOperator(
        task_id=f"get_data_{rocket_type}",
        bash_command=
            "python3 /opt/airflow/dags/module-3-airflow/spacex/load_launches.py -y {{ execution_date.year }} -o /var/data"
            if rocket_type == "all"
            else "python3 /opt/airflow/dags/module-3-airflow/spacex/load_launches.py -y {{ execution_date.year }} -r {{ params.rocket }} -o /var/data",
        params={"rocket": rocket_type},
        dag=dag
    )

    t2 = BashOperator(
        task_id=f"print_data_{rocket_type}",
        bash_command="cat /var/data/year={{ execution_date.year }}/rocket={{ params.rocket }}/data.csv",
        params={"rocket": rocket_type}, # falcon1/falcon9/falconheavy
        dag=dag
    )

    t1 >> t2
