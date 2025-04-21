import logging
import os
from datetime import datetime
import sys

def configure_logging():

    log_level=logging.INFO 
    log_format = '%(asctime)s - %(levelname)s - %(filename)s - %(message)s'
    log_dir = "logs"

    os.makedirs(log_dir, exist_ok=True) 

    current_date = datetime.now().strftime("%Y-%m-%d")
    log_filename = f"{current_date}.log"
    log_filepath = os.path.join(log_dir, log_filename)


    logger = logging.getLogger()
    logger.setLevel(log_level)


    file_handler = logging.FileHandler(log_filepath, encoding='utf-8')
    file_handler.setLevel(log_level) 
    formatter = logging.Formatter(log_format)
    file_handler.setFormatter(formatter)

    if not any(isinstance(h, logging.FileHandler) and h.baseFilename == file_handler.baseFilename for h in logger.handlers):
        logger.addHandler(file_handler)


    stream_handler_exists = any(
        isinstance(h, logging.StreamHandler) and h.stream == sys.stderr # Check stream target
        for h in logger.handlers
    )

    if not stream_handler_exists:
        stream_handler = logging.StreamHandler(sys.stderr) # Log to standard error
        stream_handler.setLevel(log_level) # Set level for this handler
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger
