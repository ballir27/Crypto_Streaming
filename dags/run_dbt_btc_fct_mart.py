import pendulum
from airflow.sdk import dag, task


@task
def run_fct_btc_mart():
    import run_dbt_airflow

    run_dbt_airflow.run_dbt(model_selected="+fct_btc")

@dag(
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
)

def fct_btc_mart():
    run_fct_btc_mart()

fct_btc_mart()
