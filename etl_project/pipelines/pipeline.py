from etl_project.connectors.exchange_rates import ExchangeRatesClient
from etl_project.connectors.postgresql import PostgresqlClient
from etl_project.assets.exchange_rates import extract_exchange_rates, transform_exchange_rates
from dotenv import load_dotenv
import yaml
from pathlib import Path
import os
from datetime import datetime, timedelta, date
from sqlalchemy import MetaData, Table, Column, Identity, String, Integer, DATE, DECIMAL
import pandas as pd

if __name__ == "__main__":
    load_dotenv()

    yaml_file_path = __file__.replace(".py", ".yaml")
    if Path(yaml_file_path).exists:
        with open(yaml_file_path) as yaml_file:
            yaml_config = yaml.safe_load(yaml_file)
    else:
        raise Exception(f"Missing {yaml_file_path} file!")
    
    base_url = yaml_config.get("base_url")
    base_currency = yaml_config.get("base_currency")
    
    ACCESS_KEY = os.environ.get("ACCESS_KEY")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    PORT = os.environ.get("PORT")

    postgresql_client = PostgresqlClient(
        db_server_name=SERVER_NAME,
        db_username=DB_USERNAME,
        db_password=DB_PASSWORD,
        db_port=PORT,
        db_database=DATABASE_NAME
    )

    meta = MetaData()
    rates_table_name = "rates"
    rates_table = Table(rates_table_name,
          meta,
          Column("id", Integer, Identity(), primary_key=True),
          Column("currency", String(3)),
          Column("rate", DECIMAL(15, 6)),
          Column("base_currency", String(3)),
          Column("date", DATE)
    )
    
    postgresql_client.create_table_if_not_exists(meta=meta, table=rates_table)

    exchange_rate_client = ExchangeRatesClient(api_base_url=base_url, api_access_key=ACCESS_KEY)

    last_update = postgresql_client.execute_scalar(query="SELECT MAX(date) AS last_updated FROM rates")
    
    date_requested = datetime.now()
    
    if last_update is None:
        date_requested = (datetime.now() - timedelta(days=1))
    elif last_update.date() < datetime.now().date():
        date_requested = last_update + timedelta(days=1)
    
    df_forex = extract_exchange_rates(exchange_rate_client=exchange_rate_client, base_currency=base_currency, date_requested=date_requested)

    if df_forex is not None:
        df_forex_transformed = transform_exchange_rates(df=df_forex, base_currency=base_currency, date=date_requested)

    if df_forex_transformed is not None:
        records_affected = postgresql_client.upsert(table=rates_table, data=df_forex_transformed.to_dict(orient="records")).rowcount
    
        print(records_affected)


    

    
    