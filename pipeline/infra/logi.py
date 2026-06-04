import logging


def get_logger(logger_name: str) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(filename)s | %(message)s",
    )
    return logging.getLogger(logger_name)
