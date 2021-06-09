import logging
from logging import StreamHandler, handlers
from pathlib import Path

def setup_logger(logger,name):
    formatter = logging.Formatter(f'[%(levelname)-s][db-connection][%(asctime)s]: %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    fh = logging.FileHandler(name)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(stream_handler)
    logger.setLevel(logging.INFO)