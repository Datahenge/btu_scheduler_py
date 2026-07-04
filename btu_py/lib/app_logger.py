"""btu_py/lib/app_logger.py"""

import logging


def build_new_logger(logger_name: str, logging_level: str) -> logging.Logger:
	logger = logging.getLogger(logger_name)
	logger.setLevel(logging.getLevelName(logging_level))
	logger.handlers = []
	logger.propagate = False
	handler = logging.StreamHandler()
	handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
	logger.addHandler(handler)
	return logger
