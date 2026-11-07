"""A failed call to Frappe (for example during a bench restart) must not stop the daemon's loops."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from btu_scheduler.daemon import coroutines


class _StopLoop(Exception):
	pass


def _config():
	return SimpleNamespace(full_refresh_internal_secs=-1, scheduler_polling_interval=30)


def test_producer_logs_a_failed_refill_and_keeps_running():
	with (
		patch.object(coroutines, "load_config", _config),
		patch.object(coroutines.scheduler, "queue_full_refill", AsyncMock(side_effect=ConnectionError("refused"))),
		patch.object(coroutines.asyncio, "sleep", AsyncMock(side_effect=_StopLoop)),
		patch.object(coroutines, "log") as log,
	):
		with pytest.raises(_StopLoop):
			asyncio.run(coroutines.internal_queue_producer(asyncio.Queue()))
	log.error.assert_called_once()


def test_review_logs_a_failed_pass_and_still_writes_the_heartbeat(tmp_path):
	heartbeat = tmp_path / "heartbeat"
	sleeps = AsyncMock(side_effect=[None, _StopLoop])  # the start-up delay, then stop after one pass
	with (
		patch.object(coroutines, "load_config", _config),
		patch.object(
			coroutines.scheduler,
			"check_and_run_eligible_task_schedules",
			AsyncMock(side_effect=ConnectionError("refused")),
		),
		patch.object(coroutines.asyncio, "sleep", sleeps),
		patch.object(coroutines, "HEARTBEAT_FILE", heartbeat),
		patch.object(coroutines, "log") as log,
	):
		with pytest.raises(_StopLoop):
			asyncio.run(coroutines.review_next_execution_times(asyncio.Queue()))
	log.error.assert_called_once()
	assert heartbeat.exists()
