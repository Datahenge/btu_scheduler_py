""" app_logger.py """

import logging
import pathlib


def build_new_logger(logger_name, logfile_path, stream_to_terminal=False):

	logger = logging.getLogger(logger_name)
	logger.level = logging.DEBUG
	logger.handlers = []
	logger.propagate = False  # prevents automatically writing to STDOUT

	# Create a Formatter
	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
	# Create a File Handler
	handler_file = logging.FileHandler(filename=pathlib.Path(logfile_path).resolve(),
	                                   mode='a',
									   encoding='utf-8')
	handler_file.setFormatter(formatter)
	logger.addHandler(handler_file)  # finally, add the handler to the custom logger

	if stream_to_terminal:
		handler_stream = logging.StreamHandler()
		logger.addHandler(handler_stream)  # finally, add the handler to the custom logger

	return logger


def new_subprocess_logger(logfile_path: str, stream_to_terminal=False):

	logger = logging.getLogger('ftp-docker-subprocess')
	logger.level = logging.DEBUG
	logger.handlers = []
	logger.propagate = False  # Important: prevents automatically writing to STDOUT

	formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')  # create a Formatter so datetime and levels are printed

	# Create a File Handler
	handler_file = logging.FileHandler(filename=pathlib.Path(logfile_path).resolve(), mode='a', encoding='utf-8')
	handler_file.setFormatter(formatter)
	handler_file.terminator = ""  # necessary because of how we're using 'readline()' in run_subprocess
	logger.addHandler(handler_file)  # finally, add the handler to the custom logger

	if stream_to_terminal:  # optionally write to terminal
		handler_stream = logging.StreamHandler()
		handler_stream.terminator = ""  # necessary because of how we're using 'readline()' in run_subprocess
		formatter = logging.Formatter('%(levelname)s - %(message)s')
		handler_stream.setFormatter(formatter)
		logger.addHandler(handler_stream)  # finally, add the handler to the custom logger

	return logger
