from loguru import logger

def get_logger(name):
    logger.add(f"{name}.log", rotation="500 MB")
    return logger
