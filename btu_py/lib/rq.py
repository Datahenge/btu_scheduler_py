""" btu_py/lib/rq.py """

import redis
import rq

import btu_py
from btu_py.lib.utils import whatis


def create_connection():

	if not btu_py.get_config().as_dictionary():
		raise RuntimeError("Application configuration is not loaded.")

	decoded_connection = redis.Redis(
		host= btu_py.get_config().as_dictionary()["rq_host"],
		port= btu_py.get_config().as_dictionary()["rq_port"],
		decode_responses=True)
	return decoded_connection


def enqueue_job_immediate(job_id: str):

	redis_conn = create_connection()
	# this_job = read_job_by_id(job_id)
	job: rq.job.Job = rq.job.Job.fetch(job_id, connection=redis_conn)

	# 1. Add the queue name to 'rq:queues'.
	queue_key: str = f"rq:queue:{job.origin}"
	some_result = redis_conn.sadd("rq:queues", queue_key)
	if not some_result:
		print("Error during enqueue_job_immediate()")
		raise IOError(some_result)

	# 2. Push the job onto the queue.
	# NOTE: The return value of 'rpush' is an integer, representing the length of the List, after the completion of the push operation.
	push_result = redis_conn.rpush(queue_key, job_id)
	whatis(push_result)
	if not push_result:
		raise IOError(push_result)
	print(f"Enqueued job '{job_id}' for immediate execution. Length of list after 'rpush' operation: {job_id}")
