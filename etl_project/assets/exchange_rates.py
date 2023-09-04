from etl_project.connectors.exchange_rates import ExchangeRatesClient
from datetime import datetime, date
import pandas as pd

def extract_exchange_rates(
        exchange_rate_client: ExchangeRatesClient,
        base_currency: str,
        date_requested: date
    ) -> pd.DataFrame:
    """
    Perform extraction by calling the exchange rates connector class

    Usage example:
        extract_exchange_rates(exchange_rate_client=exchange_rate_client, base_currency='EUR', date_requested=datetime(2023, 1, 1).date())

    Returns:
        A DataFrame with currencies and their rates against the base currency
          currency        rate base_currency        date
        0      AED    3.983947           EUR  2023-08-29
        1      AFN   90.049247           EUR  2023-08-29


    Args:
        exchange_rate_client: ExchangeRatesClient object
        base_currency: currency against which the rates are returned
        date_requested: the date for which the rates are to be returned
    """


    dictRates = exchange_rate_client.get_rates(
        base_currency=base_currency,
        date=date_requested.strftime("%Y-%m-%d")
    )

    if len(dictRates) > 0:
        df = pd.DataFrame(dictRates.items(), columns=["currency", "rate"])
        return df

def transform_exchange_rates(df: pd.DataFrame, base_currency: str, date: date):
        
    df["base_currency"] = base_currency
    df["date"] = date
    return df