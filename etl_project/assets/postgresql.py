from sqlalchemy.engine import Engine
from jinja2 import Environment

class SqlTransform:
    def __init__(self, engine: Engine, environment: Environment, table_name: str):
        self.engine = engine
        self.environment = environment
        self.table_name = table_name
        self.template = self.environment.get_template(f"{table_name}.sql")

    def drop_table_if_exists(self) -> None:
        """
        Drops the table if it exists.
        """
        exec_sql = f"""
            drop table if exists {self.table_name};
        """
        self.engine.execute(exec_sql)

    def alter_table(self) -> None:
        """
        Adds a constraint on the table for referencing. Obtains value through the sql jinja templates
        """

        source_table_name = self.template.make_module().config.get("source_table_name")
        constraint_type = self.template.make_module().config.get("constraint_type").lower()
        constraint_key = self.template.make_module().config.get("constraint_key")
        reference_key = None

        assert constraint_type == "primary" or constraint_type == "foreign", f"constraint type is invalid | value of reference_key:{constraint_type}"
        
        if constraint_type.lower() == "primary":
            exec_sql = f"""
                alter table {self.table_name} add constraint pk_{self.table_name} primary key ({constraint_key});
            """
        
        else:
            constraint_key = self.template.make_module().config.get("reference_key")
            assert reference_key is not None, f"reference_key parameter is needed if constraint_type = foreign | value of reference_key:{reference_key}"
            exec_sql = f"""
                alter table {self.table_name}
                    add constraint fk_{self.table_name} foreign key ({constraint_key}) references { source_table_name} ({reference_key}),
                    add constraint unique_fk_{self.table_name} unique ({constraint_key});
            """
        
        self.engine.execute(exec_sql)

    
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
