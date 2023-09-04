from sqlalchemy.engine import Engine
from jinja2 import Environment

class SqlTransform:
    def __init__(self, engine: Engine, environment: Environment, table_name: str):
        self.engine = engine
        self.environment = environment
        self.table_name = table_name
        self.template = self.environment.get_template(f"{table_name}.sql")

    def create_table_as(self) -> None:
        """
        Drops the table if it exists and creates a new copy of the table using the provided select statement.
        """
        exec_sql = f"""
            drop table if exists {self.table_name};
            create table {self.table_name} as (
                {self.template.render()}
            )
        """
        self.engine.execute(exec_sql)
