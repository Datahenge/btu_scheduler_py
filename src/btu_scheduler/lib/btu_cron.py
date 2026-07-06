"""btu_scheduler/lib/btu_cron.py"""

from dataclasses import dataclass
from datetime import datetime as DateTimeType
from zoneinfo import ZoneInfo

# Third Party
from croniter import croniter


@dataclass
class CronStruct:
	"""
	A cron expression consisting of 7 elements.
	"""

	second: str | None
	minute: str | None
	hour: str | None
	day_of_month: str | None
	month: str | None
	day_of_week: str | None
	year: str | None

	def to_string(self) -> str:
		"""
		Convert a CronStruct instance to a String.
		"""

		def value_or_wildcard(value):
			return value if value else "*"

		return "{} {} {} {} {}".format(
			value_or_wildcard(self.minute),
			value_or_wildcard(self.hour),
			value_or_wildcard(self.day_of_month),
			value_or_wildcard(self.month),
			value_or_wildcard(self.day_of_week),
		)

	@staticmethod
	def from_string(cron_string: str) -> "CronStruct":
		def nonwildcard_or_none(element: str) -> str | None:
			return None if element == "*" else element

		cron7_expression: str = cron_str_to_cron_str7(cron_string)
		vector_cron7: list[str] = cron7_expression.split(" ")
		return CronStruct(
			second=nonwildcard_or_none(vector_cron7[0]),
			minute=nonwildcard_or_none(vector_cron7[1]),
			hour=nonwildcard_or_none(vector_cron7[2]),
			day_of_month=nonwildcard_or_none(vector_cron7[3]),
			month=nonwildcard_or_none(vector_cron7[4]),
			day_of_week=nonwildcard_or_none(vector_cron7[5]),
			year=nonwildcard_or_none(vector_cron7[6]),
		)


def cron_str_to_cron_str7(cron_expression_string: str) -> str:
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
	match len(cron_elements):
		case 5:
			# Prefix with '0' for seconds, and suffix with '*' for years.
			return f"0 {cron_expression_string} *"
		case 6:
			# Assume we're dealing with a cron(5) plus Year.  So prefix '0' for seconds.
			return f"0 {cron_expression_string}"
		case 7:
			# Cron string already has 7 elements, so pass it back.
			return cron_expression_string
		case _:
			raise ValueError(
				f"Wrong quantity of elements ({len(cron_elements)}) found in cron_expression_string '{cron_expression_string}'"
			)


def tz_cron_to_utc_datetimes(
	cron_expression_string: str,
	cron_timezone: str | ZoneInfo,
	from_utc_datetime: DateTimeType | None,
	number_of_results: int = 1,
) -> list[DateTimeType]:
	"""
	Given a cron string (in local time) and a timezone, return the next N UTC execution datetimes.

	The cron string must be stored in local time — NOT UTC.  This function converts the UTC
	anchor to the schedule's local timezone before passing it to croniter, so cron positions
	(hour, minute, etc.) are interpreted as local clock time.  Each result is then converted
	back to UTC for use in Redis / RQ.

	DST is handled correctly:
	  - Spring forward gap: croniter skips impossible local times (e.g. 2:30 AM on transition day).
	  - Fall back fold: croniter fires once on the first occurrence of the ambiguous hour.
	  - "0 18 * * *" in America/New_York yields 23:00 UTC in winter and 22:00 UTC in summer.
	"""

	if not cron_timezone:
		raise ValueError("cron_timezone is required; each BTU Task Schedule record must supply its own timezone.")
	elif isinstance(cron_timezone, str):
		cron_timezone = ZoneInfo(cron_timezone)

	if not from_utc_datetime:
		from_utc_datetime = DateTimeType.now(ZoneInfo("UTC"))
	if not isinstance(from_utc_datetime, DateTimeType):
		raise TypeError(from_utc_datetime)

	# Convert UTC anchor to the schedule's local timezone.
	# croniter interprets the cron positions against the start_time's timezone, so passing a
	# timezone-aware local datetime causes it to return timezone-aware local datetimes.
	from_local_datetime = from_utc_datetime.astimezone(cron_timezone)

	cron_str = CronStruct.from_string(cron_expression_string).to_string()
	iterator = croniter(cron_str, from_local_datetime)

	utc_zone = ZoneInfo("UTC")
	results: list[DateTimeType] = []
	while len(results) < number_of_results:
		candidate = iterator.get_next(DateTimeType).astimezone(utc_zone)
		if results:
			prev = results[-1].astimezone(cron_timezone)
			cand = candidate.astimezone(cron_timezone)
			if prev.date() == cand.date() and prev.hour == cand.hour and prev.minute == cand.minute:
				continue  # fall-back DST duplicate: same local slot, discard and advance
		results.append(candidate)
	return results
