FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    dbt-core==1.10.16 \
    dbt-snowflake==1.10.5

WORKDIR /dbt

COPY . .

ENTRYPOINT ["dbt"]
