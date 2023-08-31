from dotenv import load_dotenv
import os
from etl_project.connectors.postgresql import PostgreSqlClient
from etl_project.connectors.exchange_rates import ExchangeRatesApiClient
from etl_project.assets.exchange_rates import extract
from etl_project.assets.postgresql import SqlTransform
from jinja2 import Environment, FileSystemLoader
from sqlalchemy.engine import URL


if __name__ == "__main__":
    # Load env variables
    load_dotenv()

    # TODO: Instantiate api client
    api_client = ExchangeRatesApiClient()

    # TODO: Instantiate source psql client
    source_psql_client = PostgreSqlClient(
        server_name=os.getenv("SOURCE_SERVER_NAME"),
        database_name=os.getenv("SOURCE_DATABASE_NAME"),
        username=os.getenv("SOURCE_DB_USERNAME"),
        password=os.getenv("SOURCE_DB_PASSWORD"),
        port=os.getenv("SOURCE_PORT")
    )


    # Extract
    # NOTE: Assumed that data is a list of dicts
    # Extract from api client to source psql client
    data = extract(
        api_client = api_client,
        psql_client = source_psql_client,
    )

    # Transform and Load
    # TODO: Set up reading of Jinja templates
    target_postgresql_client = PostgreSqlClient(
        server_name=os.getenv("TARGET_SERVER_NAME"),
        database_name=os.getenv("TARGET_DATABASE_NAME"),
        username=os.getenv("TARGET_DB_USERNAME"),
        password=os.getenv("TARGET_DB_PASSWORD"),
        port=os.getenv("TARGET_PORT")
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
