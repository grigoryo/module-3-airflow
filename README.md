# MODULE-3-AIRFLOW

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
