from loguru import logger
import time

class PipelineLogging:
    def __init__(self, pipeline_name: str, log_path: str):
        self.pipeline_name = pipeline_name
        timestamp = time.strftime('%Y%m%d_%H%M%S', time.localtime(time.time()))
        self.file_path = f"{log_path}/{pipeline_name}_{timestamp}.log"
        logger.add(self.file_path, level="INFO", rotation="50 MB", retention="10 days")

    def log_to_file(self, message: str):
        logger.info(message)

    def get_logs(self) -> str:
        with open(self.file_path, "r") as file:
            return "".join(file.readlines())
