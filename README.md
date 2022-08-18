# airflow

## Setup

```git clone https://github.com/atidorfa/airflow```

```cd airflow```

### Install virtualenv

```pip install virtualenv```

```virtualenv airflow_venv```

```source /full-path/to/venv/bin/activate```

## IMPORTANT set the AIRLFLOW_HOME

```export AIRFLOW_HOME=$PWD/airflow```

## Install airflow and dependencies (im using python 3.8)

```pip install "apache-airflow==2.3.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.3.3/constraints-3.8.txt"```

### Install dependencies

```sudo apt install default-libmysqlclient-dev```

```pip install apache-airflow-providers-mysql```

## All ready

```airflow standalone```

## Manual configuration

If you want to run the individual parts of Airflow manually rather than using the all-in-one standalone command, you can instead run:

```airflow db init```

```
airflow users create \
    --username airflow \
    --firstname Peter \
    --lastname Parker \
    --role Admin \
    --email spiderman@superhero.org
```

```airflow webserver --port 8080```

```airflow scheduler```
