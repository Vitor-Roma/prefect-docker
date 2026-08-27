# app/flows/sales.py
from datetime import timedelta

from prefect import flow, task
from prefect.cache_policies import INPUTS, TASK_SOURCE
from prefect.logging import get_run_logger


@task(
    retries=3,
    retry_delay_seconds=5,
    cache_policy=INPUTS + TASK_SOURCE,
    cache_expiration=timedelta(hours=1)
)
def get_currency(currency_id: int) -> str:
    logger = get_run_logger()
    currency_list = {
        1: "Euro",
        2: "Dolar",
        3: "Real",
        4: "Pesos"
    }
    if currency_id in currency_list:
        logger.info(f"{currency_id} -> {currency_list[currency_id]}")
        return currency_list[currency_id]
    else:
        raise ValueError(f"Money id: {currency_id} not found in database")


@flow
def sales_pipeline(currency_id_list: list[int]):
    logger = get_run_logger()
    logger.info(f"Starting sales pipeline, currency_ids: {currency_id_list}")
    currency = get_currency.map(currency_id_list)

    logger.info(f"Sales pipeline finished")

    return currency