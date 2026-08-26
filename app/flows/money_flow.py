# app/flows/sales.py

from prefect import flow, task


@task(
    retries=3,
    retry_delay_seconds=5,
)
def get_currency(currency_id: int) -> str:
    currency_list = {
        1: "Euro",
        2: "Dolar",
        3: "Real",
        4: "Pesos"
    }
    if currency_id in currency_list:
        return currency_list[currency_id]
    else:
        raise ValueError(f"Money id: {currency_id} not found in database")


@flow
def sales_pipeline(currency_id: int):
    currency = get_currency(currency_id)

    return currency