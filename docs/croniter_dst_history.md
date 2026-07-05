# croniter DST Bug: Test Correctness Record

## Summary

Two unit tests in `TestDSTSpringForward` were failing against croniter 6.0.0.
**The tests were correct. croniter was wrong.**

The failures were not caused by BTU Scheduler's implementation or test expectations.
They were caused by a long-standing DST bug in the croniter library that was finally
fixed — via a complete rewrite — in croniter 6.1.0.

---

## The Failing Tests

Both tests are in `src/btu_scheduler/tests/test_btu_cron.py`, class `TestDSTSpringForward`.

The scenario:

- Cron expression: `"30 2 * * *"` (2:30 AM, daily)
- Timezone: `America/New_York` (Eastern)
- Start time: `2026-03-08 06:59 UTC` = `01:59 AM EST`
- Spring forward occurs: `2026-03-08 07:00 UTC` → clocks jump `2:00 AM → 3:00 AM`
- `2:30 AM` on `2026-03-08` **does not exist** in Eastern time

**Expected behavior:** Skip the non-existent 2:30 AM on the spring-forward day entirely,
and return the next valid occurrence: `2026-03-09 02:30 AM EDT` = `2026-03-09 06:30 UTC`.

**croniter 6.0.0 actual behavior:** Returns `2026-03-09 01:30 AM EDT` = `2026-03-09 05:30 UTC`.

The result is off by exactly one hour — a classic DST offset error.

---

## Was This Always Broken, or a Regression?

**It was always broken**, in the sense that correct DST edge-case handling was never
fully achieved throughout the taichino-era codebase. This is not a regression story.

The DST history in croniter:

| Version | Date | Event |
|---|---|---|
| 0.3.5 and earlier | ~2014 | DST completely broken (Issue #35); behavior depended on system timezone |
| 0.3.16 | 2017-03-15 | First attempt: "DST support" added |
| 0.3.18–0.3.20 | 2017 | Multiple DST fix iterations |
| 0.3.32 | 2020-05-27 | Additional DST tests and boundary fixes |
| — | October 2020 | Issue #147 opened: wrong datetimes still produced at DST transitions |
| 6.0.0 | 2024-12-17 | Project transferred to pallets-eco; timestamp rework, but no DST logic change |
| **6.1.0** | **2026-03-14** | **"Fix DST handling by rewriting the DST logic."** — Benjamin Drung |
| 6.2.3 | 2026-07-02 | Latest stable release |

The original taichino/croniter codebase accumulated multiple partial DST fixes over
eight years without ever arriving at a complete solution. Issue #147 (filed in 2020)
was still open and unresolved when the project transferred to pallets-eco. Benjamin
Drung then did a ground-up rewrite of the DST logic in 6.1.0, which is the version
that first produces the correct result for these test cases.

The test comment `# Requires croniter >= 6.0.0 for correct DST gap handling` was
optimistic by one version. The correct minimum is **>= 6.1.0**.

---

## Resolution

Update the `pyproject.toml` dependency from:

```
"croniter==6.0.0",
```

to:

```
"croniter>=6.1.0",
```

After upgrading, all 14 tests in `test_btu_cron.py` pass.
