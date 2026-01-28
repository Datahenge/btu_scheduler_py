""" btu_py/lib/sql.py """

from databases import Database
from btu_py import get_config

# Global database instance (initialized on first use)
_database_instance: Database = None


def _quote_identifier(identifier: str, db_type: str) -> str:
	"""
	Quote a database identifier based on the database type.
	
	PostgreSQL uses double quotes: "identifier"
	MariaDB/MySQL uses backticks: `identifier`
	
	Args:
		identifier: The identifier to quote (table name, column name, etc.)
		db_type: The database type ('postgres' or 'mariadb')
	
	Returns:
		The quoted identifier
	"""
	if db_type == "postgres":
		return f'"{identifier}"'
	elif db_type == "mariadb":
		return f"`{identifier}`"
	else:
		raise ValueError(f"Unsupported database type for identifier quoting: {db_type}")


async def get_database() -> Database:
	"""
	Get or create the database connection instance.
	The database instance is created once and reused.
	"""
	global _database_instance
	
	if _database_instance is None:
		config = get_config()
		connection_string = config.get_sql_connection_string()
		_database_instance = Database(connection_string)
		await _database_instance.connect()
	
	return _database_instance


async def create_connection():
	"""
	Create a connection to the database.
	This function is kept for backward compatibility but now returns a Database instance.
	The actual connection is managed by the databases library.
	"""
	return await get_database()


async def get_task_schedule_by_id(task_schedule_id: str) -> dict:
	"""
	Returns a single Task Schedule row from the Frappe SQL database.
	"""
	config = get_config()
	db_type = config.get_sql_type()
	quote = lambda x: _quote_identifier(x, db_type)
	
	query_string = f"""
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
			{quote("tabBTU Task Schedule")}		AS TaskSchedule

		INNER JOIN
			{quote("tabSingles")}	AS Configuration
		ON
			Configuration.doctype = 'BTU Configuration'
		AND Configuration.{quote("field")} = 'cron_time_zone'
	
		WHERE
			TaskSchedule.name = :task_schedule_id

		LIMIT 1;
		"""

	database = await get_database()
	sql_row = await database.fetch_one(
		query_string,
		values={"task_schedule_id": task_schedule_id}
	)
	return sql_row


async def get_task_by_id(task_id: str) -> dict:
	"""
	Returns a single BTU Task row from the Frappe SQL database.
	"""
	config = get_config()
	db_type = config.get_sql_type()
	quote = lambda x: _quote_identifier(x, db_type)
	
	query_string = f"""
		SELECT
			name 				AS task_key, 
			desc_short,
			desc_long,
			arguments,
			function_string 	AS path_to_function,
			max_task_duration 
		FROM
			{quote("tabBTU Task")} 
		WHERE
			name = :task_id
		LIMIT 1;
	"""
	
	database = await get_database()
	sql_row = await database.fetch_one(
		query_string,
		values={"task_id": task_id}
	)
	return sql_row


async def get_enabled_tasks() -> list:
	"""
	Returns a list of all enable BTU Task records from Frappe SQL database.
	"""
	config = get_config()
	db_type = config.get_sql_type()
	quote = lambda x: _quote_identifier(x, db_type)

	query_string = f"""
		SELECT
			 name
			,desc_short
		FROM
			{quote("tabBTU Task")}
		WHERE
			docstatus = 1
		AND task_type = 'Persistent';
	"""
	
	database = await get_database()
	sql_rows = await database.fetch_all(query_string)
	return sql_rows


async def get_enabled_task_schedules() -> list:
	"""
	Returns a list of all enable BTU Task Schedule records from Frappe SQL database.
	"""
	config = get_config()
	db_type = config.get_sql_type()
	quote = lambda x: _quote_identifier(x, db_type)
	
	query_string = f"""
		SELECT
			 name			AS schedule_key
			,task			AS task_key
		FROM
			{quote("tabBTU Task Schedule")}
		WHERE
			enabled = 1;
	"""
	
	database = await get_database()
	sql_rows = await database.fetch_all(query_string)
	return sql_rows
