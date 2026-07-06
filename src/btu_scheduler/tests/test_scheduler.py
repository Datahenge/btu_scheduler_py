"""
Unit tests for TSIK, RQScheduledTask, and _dst_fired_cache_key in scheduler.py.

No running services required — all tests operate on pure data structures.
"""

import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from btu_scheduler.lib.scheduler import TSIK, RQScheduledTask, _dst_fired_cache_key

UTC = ZoneInfo("UTC")
EASTERN = ZoneInfo("America/New_York")

SAMPLE_ID = "TS-000003"
SAMPLE_TS = 1742489940
SAMPLE_KEY = f"{SAMPLE_ID}|{SAMPLE_TS}"


class TestTSIK(unittest.TestCase):

	def test_task_schedule_id_extraction(self):
		self.assertEqual(TSIK(SAMPLE_KEY).task_schedule_id(), SAMPLE_ID)

	def test_unix_timestamp_extraction(self):
		self.assertEqual(TSIK(SAMPLE_KEY).next_execution_as_unix_timestamp(), SAMPLE_TS)

	def test_datetime_utc_is_utc_aware(self):
		dt = TSIK(SAMPLE_KEY).next_execution_as_datetime_utc()
		self.assertIsNotNone(dt.tzinfo)
		self.assertEqual(dt.utcoffset().total_seconds(), 0)

	def test_datetime_utc_matches_timestamp(self):
		dt = TSIK(SAMPLE_KEY).next_execution_as_datetime_utc()
		self.assertEqual(int(dt.timestamp()), SAMPLE_TS)

	def test_from_tuple_truncates_fractional_seconds(self):
		tsik = TSIK.from_tuple(SAMPLE_ID, 1742489940.9)
		self.assertEqual(tsik.next_execution_as_unix_timestamp(), SAMPLE_TS)

	def test_from_tuple_produces_correct_key(self):
		tsik = TSIK.from_tuple(SAMPLE_ID, SAMPLE_TS)
		self.assertEqual(tsik.key, SAMPLE_KEY)

	def test_str_includes_task_schedule_id(self):
		self.assertIn(SAMPLE_ID, str(TSIK(SAMPLE_KEY)))


class TestRQScheduledTask(unittest.TestCase):

	def _make(self, task_id=SAMPLE_ID, unix_ts=SAMPLE_TS):
		return RQScheduledTask.from_tuple(task_id, unix_ts)

	def test_to_tsik_format(self):
		self.assertEqual(self._make().to_tsik(), SAMPLE_KEY)

	def test_from_tsik_preserves_task_schedule_id(self):
		task = RQScheduledTask.from_tsik(TSIK(SAMPLE_KEY))
		self.assertEqual(task.task_schedule_id, SAMPLE_ID)

	def test_from_tsik_preserves_unix_timestamp(self):
		task = RQScheduledTask.from_tsik(TSIK(SAMPLE_KEY))
		self.assertEqual(task.next_execution_as_unix_timestamp, SAMPLE_TS)

	def test_from_tsik_datetime_is_utc_aware(self):
		task = RQScheduledTask.from_tsik(TSIK(SAMPLE_KEY))
		self.assertEqual(task.next_execution_as_datetime_utc.utcoffset().total_seconds(), 0)

	def test_from_tsik_raises_type_error_on_wrong_type(self):
		with self.assertRaises(TypeError):
			RQScheduledTask.from_tsik("not-a-tsik")

	def test_from_tuple_matches_from_tsik(self):
		via_tsik = RQScheduledTask.from_tsik(TSIK(SAMPLE_KEY))
		via_tuple = RQScheduledTask.from_tuple(SAMPLE_ID, SAMPLE_TS)
		self.assertEqual(via_tsik, via_tuple)

	def test_to_tsik_roundtrip(self):
		task = self._make()
		self.assertEqual(task.to_tsik(), SAMPLE_KEY)

	def test_sort_by_id_ascending(self):
		tasks = [self._make("TS-000003"), self._make("TS-000001"), self._make("TS-000002")]
		ids = [t.task_schedule_id for t in RQScheduledTask.sort_list_by_id(tasks)]
		self.assertEqual(ids, sorted(ids))

	def test_sort_by_datetime_ascending(self):
		tasks = [self._make("TS-A", 1742489943), self._make("TS-B", 1742489941), self._make("TS-C", 1742489942)]
		timestamps = [t.next_execution_as_unix_timestamp for t in RQScheduledTask.sort_list_by_next_datetime(tasks)]
		self.assertEqual(timestamps, sorted(timestamps))

	def test_sort_preserves_all_elements(self):
		tasks = [self._make(f"TS-{i:06d}") for i in range(5, 0, -1)]
		self.assertEqual(len(RQScheduledTask.sort_list_by_id(tasks)), 5)


class TestDstFiredCacheKey(unittest.TestCase):
	"""
	Fall-back dates used:
	  2026-11-01: clocks fall back 2:00 AM EDT → 1:00 AM EST (6:00 UTC).
	  1:00–1:59 AM Eastern exists twice: once as EDT (UTC-4, before 06:00 UTC)
	  and once as EST (UTC-5, after 06:00 UTC).
	"""

	def test_key_contains_task_schedule_id(self):
		utc_dt = datetime(2026, 11, 1, 5, 30, 0, tzinfo=UTC)
		key = _dst_fired_cache_key("TS-000001", utc_dt, EASTERN)
		self.assertIn("TS-000001", key)

	def test_key_contains_local_date(self):
		# 05:30 UTC = 01:30 AM EDT on 2026-11-01 (before fall-back at 06:00 UTC)
		utc_dt = datetime(2026, 11, 1, 5, 30, 0, tzinfo=UTC)
		key = _dst_fired_cache_key("TS-000001", utc_dt, EASTERN)
		self.assertIn("2026-11-01", key)

	def test_key_contains_local_time(self):
		# 05:30 UTC = 01:30 AM EDT
		utc_dt = datetime(2026, 11, 1, 5, 30, 0, tzinfo=UTC)
		key = _dst_fired_cache_key("TS-000001", utc_dt, EASTERN)
		self.assertIn("01:30", key)

	def test_fall_back_both_occurrences_share_same_key(self):
		# The guard's entire purpose: suppress the second fire of the same local slot.
		# 05:30 UTC → 01:30 AM EDT (fold=0); 06:30 UTC → 01:30 AM EST (fold=1).
		# Both must yield the same cache key so the second fire is detected and blocked.
		first_utc = datetime(2026, 11, 1, 5, 30, 0, tzinfo=UTC)
		second_utc = datetime(2026, 11, 1, 6, 30, 0, tzinfo=UTC)
		key1 = _dst_fired_cache_key("TS-000001", first_utc, EASTERN)
		key2 = _dst_fired_cache_key("TS-000001", second_utc, EASTERN)
		self.assertEqual(key1, key2)

	def test_different_minutes_produce_different_keys(self):
		dt1 = datetime(2026, 11, 1, 5, 30, 0, tzinfo=UTC)
		dt2 = datetime(2026, 11, 1, 5, 45, 0, tzinfo=UTC)
		key1 = _dst_fired_cache_key("TS-000001", dt1, EASTERN)
		key2 = _dst_fired_cache_key("TS-000001", dt2, EASTERN)
		self.assertNotEqual(key1, key2)

	def test_different_task_ids_produce_different_keys(self):
		utc_dt = datetime(2026, 11, 1, 5, 30, 0, tzinfo=UTC)
		key1 = _dst_fired_cache_key("TS-000001", utc_dt, EASTERN)
		key2 = _dst_fired_cache_key("TS-000002", utc_dt, EASTERN)
		self.assertNotEqual(key1, key2)


if __name__ == "__main__":
	unittest.main()
