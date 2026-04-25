import pendulum
from airflow.sdk import dag, task


@task
def marts_from_postgres():
    import ducklake_add
    ducklake_add.marts_from_postgres()


@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
)
def marts_to_ducklake():
    marts_from_postgres()


marts_to_ducklake()
