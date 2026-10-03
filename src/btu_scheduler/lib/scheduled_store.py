"""btu_scheduler/lib/scheduled_store.py

Storage for the scheduler's own internal state: which Task Schedules are due next,
and which local-time slots have already fired (DST fall-back guard, see
docs/adr_dst_fall_back_protection.md).

In 'direct' connectivity mode this is a Redis sorted set, as it always has been.
In 'webserver' mode BTU has no direct Redis access, but this state was never shared
with Frappe in the first place — it's purely internal to a single scheduler daemon
process — so it lives in memory instead. No new Frappe endpoint is needed for it.
"""

import time
from typing import Protocol

import redis
import structlog

from btu_scheduler.lib.btu_rq import create_connection
from btu_scheduler.lib.config import load_config

RQ_KEY_SCHEDULED_TASKS = "btu_scheduler:task_execution_times"
log = structlog.get_logger(__name__)


class ScheduledStore(Protocol):
	def add(self, key: str, score: int) -> int: ...
	def range_le(self, max_score: float) -> list[str]: ...
	def range_all(self) -> list[str]: ...
	def remove(self, key: str) -> bool: ...
	def remove_by_prefix(self, prefix: str) -> bool: ...
	def clear(self) -> None: ...
	def dst_fired_exists(self, key: str) -> bool: ...
	def dst_fired_set(self, key: str, ttl_secs: int) -> None: ...


class RedisScheduledStore:
	"""Backs the scheduled-task sorted set and DST cache with Redis (connectivity_mode=direct)."""

	def add(self, key: str, score: int) -> int:
		redis_conn = create_connection()
		try:
			return redis_conn.zadd(RQ_KEY_SCHEDULED_TASKS, {key: score})
		except redis.exceptions.ConnectionError:
			log.error(f"ScheduledStore.add(): Cannot connect to Redis for key '{key}'.")
			return 0

	def range_le(self, max_score: float) -> list[str]:
		redis_conn = create_connection()
		try:
			return redis_conn.zrange(RQ_KEY_SCHEDULED_TASKS, 0, max_score, byscore=True)
		except redis.exceptions.ConnectionError:
			log.error("ScheduledStore.range_le(): Cannot connect to Redis; returning empty list.")
			return []

	def range_all(self) -> list[str]:
		redis_conn = create_connection()
		redis_result = redis_conn.zscan(RQ_KEY_SCHEDULED_TASKS)
		return [each[0] for each in redis_result[1]]

	def remove(self, key: str) -> bool:
		redis_conn = create_connection()
		return redis_conn.zrem(RQ_KEY_SCHEDULED_TASKS, key) == 1

	def remove_by_prefix(self, prefix: str) -> bool:
		try:
			with create_connection() as redis_conn:
				all_keys = redis_conn.zrange(RQ_KEY_SCHEDULED_TASKS, 0, -1)
				removed = False
				for each_key in all_keys:
					if each_key.startswith(prefix):
						redis_conn.zrem(RQ_KEY_SCHEDULED_TASKS, each_key)
						removed = True
				return removed
		except redis.exceptions.ConnectionError:
			log.error(f"ScheduledStore.remove_by_prefix(): Cannot connect to Redis for prefix '{prefix}'.")
			return False

	def clear(self) -> None:
		redis_conn = create_connection()
		redis_conn.zremrangebyrank(RQ_KEY_SCHEDULED_TASKS, 0, -1)

	def dst_fired_exists(self, key: str) -> bool:
		redis_conn = create_connection()
		return bool(redis_conn.exists(key))

	def dst_fired_set(self, key: str, ttl_secs: int) -> None:
		redis_conn = create_connection()
		redis_conn.setex(key, ttl_secs, "1")


class InMemoryScheduledStore:
	"""
	Backs the scheduled-task sorted set and DST cache with in-process state
	(connectivity_mode=webserver). Valid only for a single, non-horizontally-scaled
	scheduler daemon process — which is the only supported deployment shape for BTU
	Scheduler today.
	"""

	def __init__(self) -> None:
		self._tasks: dict[str, int] = {}
		self._dst_fired: dict[str, float] = {}

	def add(self, key: str, score: int) -> int:
		is_new = key not in self._tasks
		self._tasks[key] = score
		return 1 if is_new else 0

	def range_le(self, max_score: float) -> list[str]:
		return [key for key, score in self._tasks.items() if score <= max_score]

	def range_all(self) -> list[str]:
		return list(self._tasks.keys())

	def remove(self, key: str) -> bool:
		return self._tasks.pop(key, None) is not None

	def remove_by_prefix(self, prefix: str) -> bool:
		matching = [key for key in self._tasks if key.startswith(prefix)]
		for key in matching:
			del self._tasks[key]
		return bool(matching)

	def clear(self) -> None:
		self._tasks.clear()

	def _expire_dst_fired(self) -> None:
		now = time.time()
		expired = [key for key, expires_at in self._dst_fired.items() if expires_at <= now]
		for key in expired:
			del self._dst_fired[key]

	def dst_fired_exists(self, key: str) -> bool:
		self._expire_dst_fired()
		return key in self._dst_fired

	def dst_fired_set(self, key: str, ttl_secs: int) -> None:
		self._dst_fired[key] = time.time() + ttl_secs


_in_memory_store: InMemoryScheduledStore | None = None
_redis_store: RedisScheduledStore | None = None


def get_scheduled_store() -> ScheduledStore:
	"""Return the active ScheduledStore for the current connectivity_mode, memoized per mode."""
	global _in_memory_store, _redis_store  # noqa: PLW0603

	if load_config().connectivity_mode == "webserver":
		if _in_memory_store is None:
			_in_memory_store = InMemoryScheduledStore()
		return _in_memory_store

	if _redis_store is None:
		_redis_store = RedisScheduledStore()
	return _redis_store
