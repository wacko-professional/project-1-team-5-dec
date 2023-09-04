from etl_project.connectors.exchange_rates import ExchangeRatesClient
from etl_project.connectors.postgresql import PostgresqlClient
from etl_project.assets.exchange_rates import extract_exchange_rates, transform_exchange_rates
from etl_project.assets.postgresql import SqlTransform
from jinja2 import Environment, FileSystemLoader
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

    # TODO: Rename as source_psql_client
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

    # TODO: Some edge case errors here when rerunning; cannot use .date() on last_update
    elif last_update.date() < datetime.now().date():
        date_requested = last_update + timedelta(days=1)

    df_forex = extract_exchange_rates(exchange_rate_client=exchange_rate_client, base_currency=base_currency, date_requested=date_requested)

    if df_forex is not None:
        df_forex_transformed = transform_exchange_rates(df=df_forex, base_currency=base_currency, date=date_requested)

    if df_forex_transformed is not None:
        records_affected = postgresql_client.upsert(table=rates_table, data=df_forex_transformed.to_dict(orient="records")).rowcount

        print(records_affected)

     # Transform and Load
    target_postgresql_client = PostgresqlClient(
        db_server_name=os.getenv("TARGET_SERVER_NAME"),
        db_database=os.getenv("TARGET_DATABASE_NAME"),
        db_username=os.getenv("TARGET_DB_USERNAME"),
        db_password=os.getenv("TARGET_DB_PASSWORD"),
        db_port=os.getenv("TARGET_PORT")
    )

    transform_environment = Environment(loader=FileSystemLoader("etl_project/sql/transform"))

    for sql_path in transform_environment.list_templates():
        sql_template = transform_environment.get_template(sql_path)
        table_name = sql_template.make_module().config.get("table_name")

        # Node
        sql_transform = SqlTransform(
           engine=target_postgresql_client.engine,
           environment=transform_environment,
           table_name=table_name
           )

        sql_transform.create_table_as()

        ## create DAG
        #dag = TopologicalSorter()
        #dag.add()
        ## run transform
        #for node in tuple(dag.static_order()):
        #    node.create_table_as()