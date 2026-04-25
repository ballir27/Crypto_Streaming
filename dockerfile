FROM apache/airflow:latest

RUN pip install --no-cache-dir boto3 duckdb dbt-duckdb dbt-postgres loguru pydantic-settings