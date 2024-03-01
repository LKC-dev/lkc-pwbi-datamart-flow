import json
import logging
from pathlib import Path
import logging.handlers

def logger_configuration():
    """
    Configures the logger programmatically.

    Returns:
        logging.Logger: The configured logger instance.

    Raises:
        None
    """
    logger = logging.getLogger(__name__)
    if not logger.hasHandlers():
        logger.setLevel(logging.DEBUG)

        formatter = logging.Formatter(f'%(asctime)s : %(levelname)s : %(module)s : %(funcName)s : '
                                      f'%(lineno)d : pwbi_flow : log : %(message)s',
                                      datefmt='%Y-%m-%d %I:%M:%S')
        

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(formatter)

        logger_folder = Path(__file__).parent
        file_handler = logging.FileHandler(f"{logger_folder}/pwbi_flow.log")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger