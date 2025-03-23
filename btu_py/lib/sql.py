""" btu_py/lib/sql.py """

import psycopg
from psycopg.rows import dict_row
from btu_py import get_config


async def create_connection():
	"""
	Create a connection to a Postgres database.
	"""
	aconn = await psycopg.AsyncConnection.connect(
		get_config().get_sql_connection_string(),
		row_factory=dict_row
	)
	return aconn


async def get_task_schedule_by_id(task_schedule_id: str) -> dict:
	"""
	Returns a single Task Schedule row from the Frappe SQL database.
	"""
	query_string = """
		SELECT
			 TaskSchedule.name
			,TaskSchedule.task
			,TaskSchedule.task_description
			,TaskSchedule.enabled
			,CONCAT('erpnext-mybench:', TaskSchedule.queue_name) 	AS queue_name
			,TaskSchedule.redis_job_id
			,TaskSchedule.argument_overrides
			,TaskSchedule.schedule_description
			,TaskSchedule.cron_string
			,Configuration.value									AS cron_timezone
		FROM
			"tabBTU Task Schedule"		AS TaskSchedule

		INNER JOIN
			"tabSingles"	AS Configuration
		ON
			Configuration.doctype = 'BTU Configuration'
		AND Configuration."field" = 'cron_time_zone'
	
		WHERE
			TaskSchedule.name = %(task_schedule_id)s

		LIMIT 1;
		"""

	aconn = await create_connection()
	async with aconn.cursor() as acur:
		acursor: psycopg.AsyncCursor = await acur.execute(query_string, {'task_schedule_id': task_schedule_id})
		sql_row: tuple = await acursor.fetchone()
		return sql_row


async def get_task_by_id(task_id: str) -> dict:
	"""
	Returns a single BTU Task row from the Frappe SQL database.
	"""
	query_string = """
		SELECT
			name 				AS task_key, 
			desc_short,
			desc_long,
			arguments,
			function_string 	AS path_to_function,
			max_task_duration 
		FROM
			"tabBTU Task" 
		WHERE
			name = %(task_id)s
		LIMIT 1;
	"""
	aconn = await create_connection()
	async with aconn.cursor() as acur:
		acursor: psycopg.AsyncCursor = await acur.execute(query_string, {'task_id': task_id})
		sql_row: tuple = await acursor.fetchone()
		return sql_row


async def get_enabled_tasks() -> list:
	"""
	Returns a list of all enable BTU Task records from Frappe SQL database.
	"""

	query_string = """
		SELECT
			 name
			,desc_short
		FROM
			"tabBTU Task"
		WHERE
			docstatus = 1
		AND task_type = 'Persistent';
	"""
	aconn = await create_connection()
	async with aconn.cursor() as acur:
		acursor: psycopg.AsyncCursor = await acur.execute(query_string)
		sql_rows: list = await acursor.fetchall()
		return sql_rows


async def get_enabled_task_schedules() -> list:
	"""
	Returns a list of all enable BTU Task Schedule records from Frappe SQL database.
	"""
	query_string = """
		SELECT
			 name			AS schedule_key
			,task			AS task_key
		FROM
			"tabBTU Task Schedule"
		WHERE
			enabled = 1;
	"""
	aconn = await create_connection()
	async with aconn.cursor() as acur:
		acursor: psycopg.AsyncCursor = await acur.execute(query_string)
		sql_rows: list = await acursor.fetchall()
		return sql_rows
