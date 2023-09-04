from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.engine import URL, Engine
from sqlalchemy.dialects import postgresql
import pandas as pd

class PostgresqlClient:
    def __init__(self, db_server_name: str, db_username: str, db_password: str, db_port: int, db_database: str):
        self.db_server_name = db_server_name
        self.db_username = db_username
        self.db_password = db_password
        self.db_port = db_port
        self.db_database = db_database

        connection_url = URL.create(
            drivername="postgresql+pg8000",
            username=self.db_username,
            password=self.db_password,
            host=self.db_server_name,
            port = self.db_port,
            database = self.db_database
        )
        self.engine = create_engine(url=connection_url)

    def create_table_if_not_exists(self, meta: MetaData, table: Table):
        meta.create_all(bind=self.engine)

    def execute_scalar(self, query: str):
        return self.engine.execute(query).scalar()

    def upsert(self, table: Table, data: list[dict]):
        pk_columns = [pk_column.name for pk_column in table.primary_key.columns.values()]
        insert_statement = postgresql.Insert(table).values(data)
        upsert_statement = insert_statement.on_conflict_do_update(
            index_elements = pk_columns,
            set_ = {c.key:c for c in insert_statement.excluded if c.key not in pk_columns}
        )
        return self.engine.execute(upsert_statement)

    def create_all_tables(self, meta: MetaData) -> None:
        meta.create_all(self.engine)
