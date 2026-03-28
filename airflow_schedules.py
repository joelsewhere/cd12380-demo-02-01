from datetime import datetime, timedelta, timezone
from pathlib import Path
from airflow.sdk import dag, task

# ---------------------------------------------------------------------------
# Helper: derive start_date as 5 complete hours before "now"
# ---------------------------------------------------------------------------
def _five_hours_ago() -> datetime:
    """Return the UTC datetime truncated to the hour, minus 5 hours."""
    now = datetime.now(tz=timezone.utc)
    truncated = now.replace(minute=0, second=0, microsecond=0)
    return truncated - timedelta(hours=5)

default_args = {
    "owner": "data-engineering",
    "retries": 0, # No automatic retries — fail fast for the demo
    }

@dag(
    schedule="0 * * * *",   # @hourly
    start_date=_five_hours_ago(),
    default_args=default_args,
    doc_md=(Path(__file__).parent / 'README.md').as_posix(),
    description=(
        "Demonstrates catchup, intentional N-3 failure, clearing tasks, "
        "and backfilling historical runs with a date filter."
    ),
  )
def airflow_schedules():

  @task
  def ingest_data(**context):
    """Simulate pulling data for this logical interval."""
    logical_date: datetime = context["logical_date"]
    print(f"[ingest_data] Ingesting data for interval: {logical_date.isoformat()}")
    print("  → Fetched 1,000 rows from source system.")
 
  @task
  def check_not_three_behind(**context) -> None:

      logical_date: datetime = context["logical_date"]
  
      now_floored = datetime.now(tz=timezone.utc).replace(
          minute=0, second=0, microsecond=0
      )
  
      # Ensure logical_date is timezone-aware for comparison
      if logical_date.tzinfo is None:
          logical_date = logical_date.replace(tzinfo=timezone.utc)
  
      delta_hours = int((now_floored - logical_date).total_seconds() // 3600)
  
      print(f"[check_not_three_behind] logical_date : {logical_date.isoformat()}")
      print(f"[check_not_three_behind] now (floored): {now_floored.isoformat()}")
      print(f"[check_not_three_behind] delta         : {delta_hours} hour(s) behind")
  
      if delta_hours == 3:
          raise ValueError(
              f"DEMO FAILURE — this run is exactly 3 hours behind current time "
              f"(logical_date={logical_date.isoformat()}).  "
              f"Clear this task to re-run it and all downstream tasks."
          )
  
      print("  → Validation passed")
 
  @task
  def transform_data(**context) -> None:
      """Simulate a lightweight transformation step."""
      logical_date: datetime = context["logical_date"]
      print(f"[transform_data] Transforming data for interval: {logical_date.isoformat()}")
      print("  → Applied business rules; 980 rows passed validation.")
 
  @task
  def load_data(**context) -> None:
      """Simulate writing results to a data warehouse."""
      logical_date: datetime = context["logical_date"]
      print(f"[load_data] Loading data for interval: {logical_date.isoformat()}")
      print("  → 980 rows written to warehouse table `fact_events`.")
  
  ingest_data() >> check_not_three_behind() >> transform_data() >> load_data()

airflow_schedules()