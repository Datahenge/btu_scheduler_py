""" btu_py/lib/btu_cron.py """

from __future__ import annotations  # Defers evalulation of type annonations; hopefully unnecessary once Python 3.14 is released.

from dataclasses import dataclass
from datetime import datetime as DateTimeType
from zoneinfo import ZoneInfo

# Third Party
from croniter import croniter
from temporal_lib.core import make_datetime_naive, localize_datetime

# BTU
import btu_py
from btu_py.lib.utils import whatis  # pylint: disable=unused-import


NoneType = type(None)

@dataclass
class CronStruct():
	"""
	A cron expression consisting of 7 elements.
	"""

	second: [str, NoneType]
	minute: [str, NoneType]
	hour: [str, NoneType]
	day_of_month: [str, NoneType]
	month: [str, NoneType]
	day_of_week: [str, NoneType]
	year: [str, NoneType]

	def to_string(self) -> str:
		"""
		Convert a CronStruct instance to a String.
		"""
		def value_or_wildcard(value):
			return value if value else '*'

		return "{} {} {} {} {}".format(
			value_or_wildcard(self.minute),
			value_or_wildcard(self.hour),
			value_or_wildcard(self.day_of_month),
			value_or_wildcard(self.month),
			value_or_wildcard(self.day_of_week),
		)

	def to_string7(self) -> str:
		"""
		Convert a CronStruct instance to a String.
		"""
		def value_or_wildcard(value):
			return value if value else '*'

		return "{} {} {} {} {} {} {}".format(
			value_or_wildcard(self.second),
			value_or_wildcard(self.minute),
			value_or_wildcard(self.hour),
			value_or_wildcard(self.day_of_month),
			value_or_wildcard(self.month),
			value_or_wildcard(self.day_of_week),
			value_or_wildcard(self.year),
		)

	@staticmethod
	def from_string(cron_string: str) -> CronStruct:

		def nonwildcard_or_none(element: str) -> [str, NoneType]:
			return None if element == "*" else element

		cron7_expression: str = cron_str_to_cron_str7(cron_string)
		vector_cron7: list[str] = cron7_expression.split(" ")
		return CronStruct(
			second = nonwildcard_or_none(vector_cron7[0]),
			minute = nonwildcard_or_none(vector_cron7[1]),
			hour = nonwildcard_or_none(vector_cron7[2]),
			day_of_month = nonwildcard_or_none(vector_cron7[3]),
			month = nonwildcard_or_none(vector_cron7[4]),
			day_of_week = nonwildcard_or_none(vector_cron7[5]),
			year = nonwildcard_or_none(vector_cron7[6]),
		)


def cron_str_to_cron_str7 (cron_expression_string: str) -> str:
	"""
	Given a cron expression with N elements, transform into an expression with 7 elements.
	Useful because certain libraries require a 7-element cron string.
		
		0:	Seconds
		1:  Minutes
		2:  Hours
		3:  Day of Month
		4:  Month
		5:  Day of Week
		6:  Year
	"""
	cron_elements = cron_expression_string.strip().split(" ")
	match (len(cron_elements)):
		case 5:
			# Prefix with '0' for seconds, and suffix with '*' for years.
			return f"0 {cron_expression_string} *"
		case 6:
			# Assume we're dealing with a cron(5) plus Year.  So prefix '0' for seconds.
			return f"0 {cron_expression_string}"
		case 7:
			# Cron string already has 7 elements, so pass it back.
			return cron_expression_string
		case _ :
			raise ValueError(f"Wrong quantity of elements ({len(cron_elements)}) found in cron_expression_string '{cron_expression_string}'")


def tz_cron_to_utc_datetimes(cron_expression_string: str,
							 cron_timezone: [str, ZoneInfo],
							 from_utc_datetime: [DateTimeType, NoneType],
							 number_of_results: int=1) -> list[DateTimeType]:
	"""
		Given a cron string and Time Zone, what are the next set of UTC Datetime values?
		Documentation: https://docs.rs/cron/0.9.0/cron
	"""

	# NOTE 1:  This is a VERY simplistic implementation.
	#			What is truly required is something that handles Daylight Savings and related time adjustments.
	#			But it's good enough for today.

	if not cron_timezone:
		cron_timezone = btu_py.get_config().timezone()
	elif isinstance(cron_timezone, str):
		cron_timezone = ZoneInfo(cron_timezone)
	if not from_utc_datetime:
		from_utc_datetime = DateTimeType.now(ZoneInfo('UTC'))

	this_cronstruct = CronStruct.from_string(cron_expression_string)

	# 	The initial results below will be UTC datetimes.  Because that is what croniter generates.
	#
	#	Example 1:
	#		* Assume local time is 9:01 AM Pacific (1701 UTC)
	#		* Assume a cron schedule with a cadence of 30 minutes, no specific Day or Month.
	#		* The schedule will return a datetime value = 2025-03-22T17:30:00Z

	iterator = croniter(this_cronstruct.to_string(), from_utc_datetime)
	result_datetimes = [ iterator.get_next(DateTimeType) for each in range(number_of_results) ]

	# Scenario #1: If the Hour component is the entire range of hours (*), then accept the Schedule as-is.
	if isinstance(this_cronstruct.hour, NoneType) or this_cronstruct.hour == "*":
		return result_datetimes

	# Secenario 2: A specific Hour of the day.
	#	1. Strip the time zone component, so the UTC DateTime becomes a Naive Datetime.
	#	2. Change to Local Times by applying the function argument `cron_timezone`
	#	   At this point, it's as-if croniter generated a Local time in the first place.
	#	3. Finally, shift the DateTime to UTC, in preparation for integration with RQ.
	#
	#	NOTE: yes this will completely break during Daylight Savings.  For today, it's 80/20.

	modified_results = []
	for utc_datetime in result_datetimes:

		# This logic acquires the exact same Hour:Minute, but in local time.
		naive_datetime = make_datetime_naive(utc_datetime)
		tz_aware = localize_datetime(naive_datetime, cron_timezone)  # localize to config file's TimeZone
		new_utc_datetime = tz_aware.astimezone(ZoneInfo('UTC'))

		modified_results.append(new_utc_datetime)

		if utc_datetime.date().day != new_utc_datetime.date().day:
			btu_py.get_logger().debug(f"Original and new 'utc_datetime' fall on different days ({utc_datetime} vs {new_utc_datetime})")

	return modified_results
