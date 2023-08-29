from etl_project.connectors.exchange_rates import ExchangeRatesClient
from etl_project.connectors.postgresql import PostgresqlClient
from dotenv import load_dotenv
import yaml
from pathlib import Path
import os
from datetime import datetime
from sqlalchemy import MetaData, Table, Column, Identity, String, Integer, DATE, DECIMAL

def get_value_from_yaml_config(yaml_file_path: str, key: str):
    if Path(yaml_file_path).exists:
        with open(yaml_file_path) as yaml_file:
            yaml_config = yaml.safe_load(yaml_file)
            return yaml_config.get(key)
    else:
        raise Exception(f"Missing {yaml_file_path} file! Please create the yaml file with at least a `{key}` key for the pipeline name.")

if __name__ == "__main__":
    load_dotenv()
    ACCESS_KEY = os.environ.get("ACCESS_KEY")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    PORT = os.environ.get("PORT")

    yaml_file_path = __file__.replace(".py", ".yaml")
    base_url = get_value_from_yaml_config(yaml_file_path = yaml_file_path, key="base_url")
    base_currency = get_value_from_yaml_config(yaml_file_path = yaml_file_path, key="base_currency")

    exchange_rate_client = ExchangeRatesClient(api_base_url=base_url, api_access_key=ACCESS_KEY)

    current_date = datetime.now().strftime("%Y-%m-%d")
    print(current_date)

    # dictRates = exchange_rate_client.get_rates(base_currency=base_currency, date=current_date)
    # print(dictRates)

    postgresql_client = PostgresqlClient(
        db_server_name=SERVER_NAME,
        db_username=DB_USERNAME,
        db_password=DB_PASSWORD,
        db_port=PORT,
        db_database=DATABASE_NAME
    )

    # postgresql_client.execute_scalar(query="SELECT MAX(date) AS last_updated FROM rates")

    meta = MetaData()
    rates_table_name = "rates"
    rates_table = Table(rates_table_name,
          meta,
          Column("id", Integer, Identity(), primary_key=True),
          Column("currency", String(3)),
          Column("rate", DECIMAL(10, 6)),
          Column("base_currency", String(3)),
          Column("date", DATE)
    )
    
    postgresql_client.create_table(meta=meta, table=rates_table)
    


    