# Airflow Catchup & Backfill

This DAG demonstrates three key Airflow operational patterns:
 
  1. CATCHUP             — Airflow auto-schedules missed runs when a DAG is
                         unpaused or its start_date is in the past.
  2. HISTORICAL FAILURE  — The run that is exactly 3 intervals behind the
                         current logical date fails.
  3. CLEAR / RERUN       — Once the error is resolved, the downstream
                         tasks are cleared, and later Dag Runs are backfilled
 
Schedule
--------
- @hourly  (cron: "0 * * * *")
- start_date: 5 hours before the current UTC hour  →  Airflow will
- immediately create 5 queued/running catchup runs when the DAG is unpaused.

Steps
-----

1. Run dag unchanged
2. Set catchup=True
3. Delete run history
4. Unpause the dag
5. Debug failed historical task
6. Clear failed task and downstream
7. Backfill historical runs
 