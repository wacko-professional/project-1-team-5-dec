from etl_project.connectors.exchange_rates import ExchangeRatesClient
from etl_project.connectors.postgresql import PostgresqlClient
from etl_project.assets.pipeline_logging import PipelineLogging
from etl_project.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus
from etl_project.assets.exchange_rates import extract_exchange_rates, transform_exchange_rates
from etl_project.assets.postgresql import SqlTransform
from jinja2 import Environment, FileSystemLoader
from etl_project.assets.postgresql import SqlTransform
from jinja2 import Environment, FileSystemLoader
from dotenv import load_dotenv
import yaml
from pathlib import Path
import os
from datetime import datetime, timedelta, date
from sqlalchemy import MetaData, Table, Column, Identity, String, Integer, DATE, DECIMAL

if __name__ == "__main__":

    # load environment variables
    load_dotenv()

    # get config values from yaml file
    yaml_file_path = __file__.replace(".py", ".yaml")
    if Path(yaml_file_path).exists:
        with open(yaml_file_path) as yaml_file:
            yaml_config = yaml.safe_load(yaml_file)
    else:
        raise Exception(f"Missing {yaml_file_path} file!")

    base_url = yaml_config.get("base_url")
    base_currency = yaml_config.get("base_currency")
    log_path = yaml_config.get("config").get("log_folder_path")

    # create logging client
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    LOGGING_SERVER_NAME = os.environ.get("SERVER_NAME")
    LOGGING_DATABASE_NAME = os.environ.get("DATABASE_NAME")
    PORT = os.environ.get("PORT")

    postgresql_logging_client = PostgresqlClient(
        db_server_name=LOGGING_SERVER_NAME,
        db_database=LOGGING_DATABASE_NAME,
        db_username=DB_USERNAME,
        db_password=DB_PASSWORD,
        db_port=PORT
    )

    try:
        metadata_logging = MetaDataLogging(pipeline_name="exchange_rates", postgresql_client=postgresql_logging_client)
        pipeline_logger = PipelineLogging(pipeline_name="exchange_rates", log_path=log_path)
        pipeline_logger.log_to_file(message="Starting pipeline run")

        # set up environment variables
        pipeline_logger.log_to_file(message="Getting pipeline environment variables")
        ACCESS_KEY = os.environ.get("ACCESS_KEY")
        DB_USERNAME = os.environ.get("DB_USERNAME")
        DB_PASSWORD = os.environ.get("DB_PASSWORD")
        SERVER_NAME = os.environ.get("SERVER_NAME")
        DATABASE_NAME = os.environ.get("DATABASE_NAME")
        PORT = os.environ.get("PORT")

        pipeline_logger.log_to_file(message="Initialising PostgresClient instance")
        raw_psql_client = PostgresqlClient(
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

        pipeline_logger.log_to_file(message=f"Creating table {rates_table_name} if it does not exist")
        raw_psql_client.create_table_if_not_exists(meta=meta, table=rates_table)

        pipeline_logger.log_to_file(message="Creating ExchangeRatesClient instance")
        exchange_rate_client = ExchangeRatesClient(api_base_url=base_url, api_access_key=ACCESS_KEY)

        pipeline_logger.log_to_file(message="Retrieving last extract date")
        last_update = raw_psql_client.execute_scalar(query="SELECT MAX(date) AS last_updated FROM rates")

        day_count = 0

        # if no last_update value retrieved from data, set the last_date to yesterday's date and days of data to extract to 1
        # else if last_update exists and is before the current date, set the days of date to extract to the number of days since last update
        if last_update is None:
            last_update = datetime.now().date() - timedelta(days=1)
            day_count = 1
        elif last_update < datetime.now().date():
            day_count = (datetime.now().date() - last_update).days

        # limit the number of days of data to extract to 10
        if day_count > 10:
            day_count = 10

        pipeline_logger.log_to_file(message=f"Extracting rates data for the past {day_count} days")

        for date_requested in (last_update + timedelta(days=n+1) for n in range(day_count)):

            pipeline_logger.log_to_file(message=f"Extracting rates data for {date_requested}")
            df_forex = extract_exchange_rates(exchange_rate_client=exchange_rate_client, base_currency=base_currency, date_requested=date_requested)

            if df_forex is not None:
                pipeline_logger.log_to_file(message=f"Transforming rates DataFrame for {date_requested}")
                df_forex_transformed = transform_exchange_rates(df=df_forex, base_currency=base_currency, date=date_requested)

            if df_forex_transformed is not None:
                pipeline_logger.log_to_file(message=f"Loading data for {date_requested}")
                records_affected = raw_psql_client.upsert(table=rates_table, data=df_forex_transformed.to_dict(orient="records")).rowcount

                print(records_affected)

         # Transform and Load
        staging_postgresql_client = PostgresqlClient(
            db_server_name=os.getenv("TARGET_SERVER_NAME"),
            db_database=os.getenv("TARGET_DATABASE_NAME"),
            db_username=os.getenv("TARGET_DB_USERNAME"),
            db_password=os.getenv("TARGET_DB_PASSWORD"),
            db_port=os.getenv("TARGET_PORT")
        )

        transform_environment = Environment(loader=FileSystemLoader("etl_project/sql/transform"))

        pipeline_logger.log_to_file(message="Preparing transforming and loading")
        for sql_path in transform_environment.list_templates():
            sql_template = transform_environment.get_template(sql_path)
            table_name = sql_template.make_module().config.get("table_name")

            pipeline_logger.log_to_file(message=f"START: Transforming and loading table {table_name}")

            # Node
            sql_transform = SqlTransform(
               engine=staging_postgresql_client.engine,
               environment=transform_environment,
               table_name=table_name
               )

            sql_transform.create_table_as()

            pipeline_logger.log_to_file(message=f"END: Transforming and loading table {table_name} successful")

            ## create DAG
            #dag = TopologicalSorter()
            #dag.add()
            ## run transform
            #for node in tuple(dag.static_order()):
            #    node.create_table_as()

        pipeline_logger.log_to_file(message="Pipeline run successful")
        metadata_logging.log(status=MetaDataLoggingStatus.RUN_SUCCESS, logs=pipeline_logger.get_logs())

    except Exception as e:
        pipeline_logger.logger.error(f"Pipeline failed with exception {e}")
        metadata_logging.log(status=MetaDataLoggingStatus.RUN_FAILURE, logs=pipeline_logger.get_logs())
