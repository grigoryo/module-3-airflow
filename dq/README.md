# DQ

## Setup

```shell
# IMPORTANT! Current directory must be `./dq` to avoid conflicts with Airflow.
cd dq
py -3.8 -m venv .venv
source .venv/Scripts/activate
pip install -r requirements.txt
deactivate
```

## Use

```shell
cd dq
source .venv/Scripts/activate

great_expectations init
great_expectations suite demo

great_expectations suite list

great_expectations suite edit gosipenkov.stg_billing.warning
great_expectations suite edit gosipenkov.stg_issue.warning
great_expectations suite edit gosipenkov.stg_payment.warning
great_expectations suite edit gosipenkov.stg_traffic.warning
great_expectations suite edit gosipenkov.stg_user.warning

great_expectations suite edit gosipenkov.ods_billing.warning
great_expectations suite edit gosipenkov.ods_issue.warning
great_expectations suite edit gosipenkov.ods_payment.warning
great_expectations suite edit gosipenkov.ods_traffic.warning
great_expectations suite edit gosipenkov.ods_user.warning

great_expectations docs build --site-name local_site

deactivate
```
