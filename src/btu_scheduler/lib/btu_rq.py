"""btu_scheduler/lib/btu_rq.py"""

# NOTE: Deliberately naming this "btu_rq" to distinguish from the Third Party library namespace "rq"

import redis

from btu_scheduler.lib.config import load_config


def create_connection(decode_responses=True) -> redis.StrictRedis:
	"""
	Creates a connection to the Redis database.
	"""
	config = load_config()
	return redis.StrictRedis(
		host=config.rq_host,
		port=config.rq_port,
		password=config.rq_password,
		decode_responses=decode_responses,
	)
