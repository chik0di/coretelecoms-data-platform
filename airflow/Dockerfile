FROM apache/airflow:3.0.6

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

RUN pip install apache-airflow-providers-snowflake \
                 apache-airflow-providers-amazon \
                 apache-airflow-providers-docker 