# MODULE-3-AIRFLOW

**Data Engineer 2021** training course homeworks.

## Contents

### Task 3

-   Data Lake DAG: [data_lake_etl.py](data_lake_etl.py)
-   Data Lake scripts: [data_lake/sql-scripts.md](data_lake/sql-scripts.md)

### Task 4

-   DWH DAG: [dwh_etl.py](dwh_etl.py)
-   DWH scripts directory: `./dwh`

### Task 5

**Проверка качества данных на основе фреймворка Great Expectations.**

**Размещение:**

-   папка `./dq`

**Data docs:**

-   [dq/data_docs.zip](dq/data_docs.zip)

**Описание проверок:**

-   Проверками были покрыты четыре таблицы слоя ODS: `ods_billing`,
    `ods_issue`, `ods_payment`, `ods_traffic`.

-   Сперва были сгенерированы демонстрационные проверки при помощи команд
    `great_expectations init`, `great_expectations suite demo`.

-   Далее для всех колонок были применены проверки на непустое значение
    (например, для `user_id`), т.к. в нашем наборе данных пустые поля
    отсутствуют.

-   Были убраны проверки на основе агрегирующих функций, т.к.:

    -   в ряде случаев они применялись к полям, не являющимся мерами, например,
        `billing_period`,

    -   в прочих случаях генератор случайных чисел, которым наполнялся наш
        набор данных, давал слишком синтетические значения и такая проверка
        выглядела синтетически.

-   Были добавлены проверки на границы допустимых значений для объемов,
    сумм (с целью отлавливать аномальные выбросы значений) и для
    `billing_period` (как простой вариант проконтролировать хотя бы год).

-   Были добавлены REGEX проверки для полей с лицевым счетом, IP-адресом,
    номером телефона и т.п. Возможно, они будут слишком медленными в реальной
    ситуации, но на небольшом наборе данных гарантируют полное отсутствие
    ошибок в формате значений (но не защищают от смысловых ошибок, таких как
    IP из недопустимого диапазона, неправильный префикс у номера телефона).

## Development environment setup

### Windows

>   Airflow Version: `1.10.14`
>
>   Install docs: <https://pypi.org/project/apache-airflow/1.10.14/>

Upgrade to `setproctitle 1.2.2`
to avoid <https://github.com/dvarrazzo/py-setproctitle/issues/89> issue
_"Could not build wheels for setproctitle which use PEP 517 and cannot be installed directly"_:

1.  Download <https://raw.githubusercontent.com/apache/airflow/constraints-1.10.14/constraints-3.8.txt>.

2.  Save it as `airflow-constraints-1.10.14-3.8-fixed.txt`.

3.  Change line `setproctitle==1.2` to `setproctitle==1.2.2`.

Setup virtual environment:

```shell
py -3.8 -m venv .venv
source .venv/Scripts/activate
pip install pip==20.2.4
# Uninstall error in preceeding command is an expected behaviour.
pip install apache-airflow==1.10.14 --constraint "./airflow-constraints-1.10.14-3.8-fixed.txt"
deactivate
```

## VS Code

Add desired options from `./.vscode/settings.sample.json`
to your `./.vscode/settings.json`
