from sqlalchemy import create_engine
from sqlalchemy.engine import URL

class PostgreSqlClient:
    """
    A boilerplate psql client
    """

    def __init__(self,
                 server_name: str,
                 database_name: str,
                 username: str,
                 password: str,
                 port: int = 5432
                 ):
        self.host_name = server_name
        self.database_name = database_name
        self.username = username
        self.password = password
        self.port = port

        connection_url = URL.create(
            drivername = "postgresql+pg8000",
            username = self.username,
            password = self.password,
            host = self.host_name,
            port = self.port,
            database = self.database_name,
        )

        self.engine = create_engine(connection_url)
