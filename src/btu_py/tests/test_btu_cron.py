"""
Unit tests for btu_py.lib.btu_cron.tz_cron_to_utc_datetimes().

Run with:  python -m pytest btu_py/tests/test_btu_cron.py -v
           (from the btu_scheduler_py project root, with dependencies installed)

All tests supply cron_timezone and from_utc_datetime explicitly, so no
btu_py configuration file or running services are required.

US Eastern DST dates used below:
  Spring forward: 2026-03-08  2:00 AM EST → 3:00 AM EDT  (7:00 UTC)
  Fall back:      2026-11-01  2:00 AM EDT → 1:00 AM EST  (6:00 UTC)
"""

import unittest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from btu_py.lib.btu_cron import tz_cron_to_utc_datetimes

UTC     = ZoneInfo("UTC")
EASTERN = ZoneInfo("America/New_York")
PACIFIC = ZoneInfo("America/Los_Angeles")


def _run(cron: str, tz, start_utc: datetime, n: int = 1):
	"""Thin helper: call tz_cron_to_utc_datetimes and return results."""
	return tz_cron_to_utc_datetimes(cron, tz, start_utc, n)


class TestNormalUTCConversion(unittest.TestCase):
	"""Non-DST cases with pinned expected values."""

	def test_winter_6pm_eastern_is_2300_utc(self):
		# 2026-01-15 08:01 EST → next 6 PM EST = 23:00 UTC same day
		start = datetime(2026, 1, 15, 13, 1, 0, tzinfo=UTC)
		result = _run("0 18 * * *", EASTERN, start)[0]
		self.assertEqual(result, datetime(2026, 1, 15, 23, 0, 0, tzinfo=UTC))

	def test_summer_6pm_eastern_is_2200_utc(self):
		# 2026-07-15 09:01 EDT → next 6 PM EDT = 22:00 UTC same day
		start = datetime(2026, 7, 15, 13, 1, 0, tzinfo=UTC)
		result = _run("0 18 * * *", EASTERN, start)[0]
		self.assertEqual(result, datetime(2026, 7, 15, 22, 0, 0, tzinfo=UTC))

	def test_10pm_eastern_winter_crosses_utc_midnight(self):
		# 10 PM EST = UTC-5 → 03:00 UTC next day
		start = datetime(2026, 1, 15, 13, 1, 0, tzinfo=UTC)
		result = _run("0 22 * * *", EASTERN, start)[0]
		self.assertEqual(result, datetime(2026, 1, 16, 3, 0, 0, tzinfo=UTC))

	def test_6pm_pacific_winter_crosses_utc_midnight(self):
		# 6 PM PST = UTC-8 → 02:00 UTC next day
		start = datetime(2026, 1, 15, 18, 1, 0, tzinfo=UTC)
		result = _run("0 18 * * *", PACIFIC, start)[0]
		self.assertEqual(result, datetime(2026, 1, 16, 2, 0, 0, tzinfo=UTC))

	def test_timezone_as_string_matches_zoneinfo_object(self):
		start = datetime(2026, 1, 15, 13, 1, 0, tzinfo=UTC)
		by_string  = _run("0 18 * * *", "America/New_York", start)[0]
		by_zoneinfo = _run("0 18 * * *", EASTERN, start)[0]
		self.assertEqual(by_string, by_zoneinfo)


class TestDSTCoreInvariant(unittest.TestCase):
	"""
	The central DST correctness claim:
	the same local-time cron yields different UTC offsets in winter vs summer.
	"""

	def test_same_cron_produces_different_utc_across_dst_boundary(self):
		winter = datetime(2026, 1, 15, 13, 1, 0, tzinfo=UTC)
		summer = datetime(2026, 7, 15, 13, 1, 0, tzinfo=UTC)
		winter_result = _run("0 18 * * *", EASTERN, winter)[0]
		summer_result = _run("0 18 * * *", EASTERN, summer)[0]
		# Summer UTC offset is 1 hour earlier than winter
		self.assertEqual(winter_result.hour - summer_result.hour, 1)

	def test_results_are_always_utc_aware(self):
		start = datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC)
		results = _run("0 18 * * *", EASTERN, start, 3)
		for r in results:
			self.assertEqual(r.utcoffset(), timedelta(0),
				msg=f"Result {r} has non-zero UTC offset; expected UTC-aware datetime")


class TestMultipleResults(unittest.TestCase):

	def test_daily_cron_returns_consecutive_days(self):
		start = datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC)
		results = _run("0 18 * * *", EASTERN, start, 3)
		self.assertEqual(len(results), 3)
		self.assertEqual(results[1] - results[0], timedelta(days=1))
		self.assertEqual(results[2] - results[1], timedelta(days=1))

	def test_sub_hourly_returns_correct_interval(self):
		start = datetime(2026, 6, 1, 10, 0, 0, tzinfo=UTC)
		results = _run("*/15 * * * *", EASTERN, start, 4)
		self.assertEqual(len(results), 4)
		for i in range(1, len(results)):
			self.assertEqual(results[i] - results[i - 1], timedelta(minutes=15))

	def test_all_results_strictly_after_start(self):
		start = datetime(2026, 3, 1, 10, 0, 0, tzinfo=UTC)
		results = _run("*/30 * * * *", EASTERN, start, 5)
		for r in results:
			self.assertGreater(r, start)


class TestDSTSpringForward(unittest.TestCase):
	"""
	2026-03-08: clocks spring forward 2:00 AM EST → 3:00 AM EDT (7:00 UTC).
	The 2:00–2:59 AM window does not exist in Eastern time on this day.

	Requires croniter >= 6.0.0 for correct DST gap handling.
	"""

	def test_cron_in_spring_forward_gap_skips_to_next_valid_day(self):
		# Start: 1:59 AM EST on spring-forward day (06:59 UTC)
		start = datetime(2026, 3, 8, 6, 59, 0, tzinfo=UTC)
		result = _run("30 2 * * *", EASTERN, start)[0]
		local_result = result.astimezone(EASTERN)

		# The result must NOT be on the same day as the gap
		self.assertGreater(local_result.date(), datetime(2026, 3, 8).date(),
			msg="Cron in DST gap should not produce a result on the same day as the gap")

		# The local time should match the cron pattern (hour=2, minute=30)
		self.assertEqual(local_result.hour, 2)
		self.assertEqual(local_result.minute, 30)

	def test_result_after_spring_forward_uses_edt_offset(self):
		# After spring forward, Eastern is EDT (UTC-4), so 2:30 AM EDT = 06:30 UTC
		start = datetime(2026, 3, 8, 6, 59, 0, tzinfo=UTC)
		result = _run("30 2 * * *", EASTERN, start)[0]
		local_result = result.astimezone(EASTERN)

		self.assertEqual(local_result.utcoffset(), timedelta(hours=-4),
			msg="Post-spring-forward result should have EDT offset (UTC-4)")
		# 2:30 AM EDT = 06:30 UTC on whatever day croniter lands on
		self.assertEqual(result.hour, 6)
		self.assertEqual(result.minute, 30)


class TestDSTFallBack(unittest.TestCase):
	"""
	2026-11-01: clocks fall back 2:00 AM EDT → 1:00 AM EST (6:00 UTC).
	The 1:00–1:59 AM window exists twice in Eastern time on this day.

	The scheduler should fire at the FIRST occurrence of the ambiguous hour
	(EDT, UTC-4) and not double-fire within the same local calendar day.

	Requires croniter >= 6.0.0 for correct DST fold handling.
	"""

	def test_fall_back_fires_at_first_occurrence(self):
		# Start: 5:00 UTC = 1:00 AM EDT (the first 1 AM before clocks fall back)
		start = datetime(2026, 11, 1, 5, 0, 0, tzinfo=UTC)
		result = _run("30 1 * * *", EASTERN, start)[0]

		# First 1:30 AM EDT = 05:30 UTC
		self.assertEqual(result, datetime(2026, 11, 1, 5, 30, 0, tzinfo=UTC))

	def test_fall_back_does_not_double_fire_same_day(self):
		# Request two consecutive occurrences starting at 5:00 UTC (1:00 AM EDT)
		start = datetime(2026, 11, 1, 5, 0, 0, tzinfo=UTC)
		results = _run("30 1 * * *", EASTERN, start, 2)
		r0_local = results[0].astimezone(EASTERN)
		r1_local = results[1].astimezone(EASTERN)

		# The two results must fall on different local calendar days
		self.assertNotEqual(r0_local.date(), r1_local.date(),
			msg="Daily cron must not fire twice on the same local calendar day (fall-back fold)")


if __name__ == "__main__":
	unittest.main()
