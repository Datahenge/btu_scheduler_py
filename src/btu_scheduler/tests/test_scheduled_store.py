"""
Unit tests for InMemoryScheduledStore (the connectivity_mode=webserver backing store).

No running services required — pure in-memory data structure.
"""

import unittest

from btu_scheduler.lib.scheduled_store import InMemoryScheduledStore


class TestInMemoryScheduledStoreTaskTracking(unittest.TestCase):
	def setUp(self):
		self.store = InMemoryScheduledStore()

	def test_add_new_key_reports_one_added(self):
		self.assertEqual(self.store.add("TS-000001|1000", 1000), 1)

	def test_add_existing_key_reports_zero_added(self):
		self.store.add("TS-000001|1000", 1000)
		self.assertEqual(self.store.add("TS-000001|1000", 1000), 0)

	def test_range_le_includes_keys_at_or_below_score(self):
		self.store.add("a|100", 100)
		self.store.add("b|200", 200)
		self.assertEqual(sorted(self.store.range_le(150)), ["a|100"])

	def test_range_all_returns_every_key(self):
		self.store.add("a|100", 100)
		self.store.add("b|200", 200)
		self.assertEqual(sorted(self.store.range_all()), ["a|100", "b|200"])

	def test_remove_existing_key_returns_true(self):
		self.store.add("a|100", 100)
		self.assertTrue(self.store.remove("a|100"))

	def test_remove_missing_key_returns_false(self):
		self.assertFalse(self.store.remove("missing"))

	def test_remove_by_prefix_removes_all_matching(self):
		self.store.add("TS-1|100", 100)
		self.store.add("TS-1|200", 200)
		self.store.add("TS-2|300", 300)
		removed = self.store.remove_by_prefix("TS-1")
		self.assertTrue(removed)
		self.assertEqual(self.store.range_all(), ["TS-2|300"])

	def test_clear_removes_everything(self):
		self.store.add("a|100", 100)
		self.store.clear()
		self.assertEqual(self.store.range_all(), [])


class TestInMemoryScheduledStoreDstFired(unittest.TestCase):
	def setUp(self):
		self.store = InMemoryScheduledStore()

	def test_unset_key_does_not_exist(self):
		self.assertFalse(self.store.dst_fired_exists("btu:fired:TS-1:2026-11-01:01:30"))

	def test_set_key_exists_before_ttl_expires(self):
		key = "btu:fired:TS-1:2026-11-01:01:30"
		self.store.dst_fired_set(key, ttl_secs=90_000)
		self.assertTrue(self.store.dst_fired_exists(key))

	def test_set_key_does_not_exist_after_ttl_expires(self):
		key = "btu:fired:TS-1:2026-11-01:01:30"
		self.store.dst_fired_set(key, ttl_secs=-1)  # already expired
		self.assertFalse(self.store.dst_fired_exists(key))
