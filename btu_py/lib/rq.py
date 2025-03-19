""" btu_py/lib/rq.py """

import redis
from btu_py.lib.config import AppConfig

def create_connection():

	if not AppConfig.as_dictionary():
		raise RuntimeError("Application configuration is not loaded.")

	decoded_connection = redis.Redis(
		host=AppConfig.as_dictionary()["rq_host"],
		port=AppConfig.as_dictionary()["rq_port"],
		decode_responses=True)
	return decoded_connection
