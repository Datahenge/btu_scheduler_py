"""btu_py/lib/app_logger.py"""

import logging
import pathlib


def build_new_logger(logger_name: str, logfile_path, logging_level: str, stream_to_terminal=True):
	logger = logging.getLogger(logger_name)
	logger.level = logging.getLevelName(logging_level)  # determine the Level from the application's configuration.
	logger.handlers = []
	logger.propagate = False  # prevents automatically writing to STDOUT

	# Create a Formatter
	formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
	# Create a File Handler
	handler_file = logging.FileHandler(filename=pathlib.Path(logfile_path).resolve(), mode="a", encoding="utf-8")
	handler_file.setFormatter(formatter)
	logger.addHandler(handler_file)  # finally, add the handler to the custom logger

	if stream_to_terminal:
		logger.propagate = False
		print("Note: Logger will also stream to the terminal.")
		handler_stream = logging.StreamHandler()
		handler_stream.setFormatter(formatter)
		logger.addHandler(handler_stream)  # finally, add the handler to the custom logger

	return logger
