""" btu_py/lib/tests.py """

import psycopg

def test_redis():
	"""
	Test the connection to the Redis database.
	"""
	from btu_py.lib.rq import create_connection
	conn = create_connection()
	return conn.ping()


async def test_sql():
	"""
	Test the connection to the Frappe database.
	"""
	from btu_py.lib.sql import create_connection
	query_string = "SELECT count(*) AS record_count FROM \"tabDocType\";"

	aconn = await create_connection()  # returns an object which can be used as a context.
	async with aconn.cursor() as acur:
		acursor: psycopg.AsyncCursor = await acur.execute(query_string)
		sql_row: dict = await acursor.fetchone()
		print(f"Number of records in DocType table = {sql_row['record_count']}")
