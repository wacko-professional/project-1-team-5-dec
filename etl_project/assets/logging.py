from loguru import logger

class LoggingClient:
    def __init__(self, log_path: str):
        logger.add(log_path + "/file_{time}.log", level="INFO", rotation="50 MB", retention="10 days")
        

    def log_to_file(self, message: str):
        logger.info(message)