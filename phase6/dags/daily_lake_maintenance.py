import os
import time
from datetime import datetime, timedelta

from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag, task
from airflow.timetables.trigger import CronTriggerTimetable

# --- Paths inside the Airflow container ---
# repo is mounted read-only at /opt/project so the dag can run phase4/sync_to_s3.sh
# and read the lake the spark jobs write
PROJECT_DIR = "/opt/project"
LAKE_DIR = f"{PROJECT_DIR}/data/lake"

# if the newest parquet file in a dataset is older than this, a streaming job
# has probably failed or stopped. weather stations report at least hourly, so 3h is a 
# safe margin that still catches a real outage the same night
MAX_STALENESS_HOURS = 3

@dag(
    dag_id="daily_lake_maintenance",
    # run everyday at 00:30 UTC 
    schedule=CronTriggerTimetable("30 0 * * *", timezone="UTC"),
    start_date=datetime(2026, 10, 1),
    # don't create runs for every missed day
    catchup=False,
    # one auto retry 5 min later
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    # optional input when triggering by hand: curate a specific day (backfill)
    params={
        "date": Param(
            default="",
            type=["null", "string"],
            description="Day to curate (yyyy-MM-dd). Empty = the day before the run.",
        )
    },
    tags=["weather-seismic"],
)

def daily_lake_maintenance():
    # --- Task 1: is the streaming pipeline still writing? ---
    # finds the newest parquet file per dataset and fails if it's too old
    # failing here stops the run before spending money on glue for a broken day
    @task
    def check_freshness():
        now = time.time()
        for dataset in ["weather", "seismic"]:
            newest = max(
                (
                    os.path.getmtime(os.path.join(folder, name))
                    for folder, _, files in os.walk(f"{LAKE_DIR}/{dataset}")
                    for name in files
                    if name.endswith(".parquet")
                ),
                default=0,
            )
            age_hours = (now - newest) / 3600
            print(f"{dataset}: newest file is {age_hours:.1f}h old")
            if age_hours > MAX_STALENESS_HOURS:
                raise ValueError(
                    f"{dataset} lake is stale ({age_hours:.1f}h) — is its streaming job running?"
                )
    
    # --- Task 2: upload new lake files to s3 raw ---
    # reuses the existing script the trailing space after .sh is required
    # without it airflow treats a command ending in .sh as a template FILE to 
    # load and fails with TemplateNotFound
    sync_to_s3 = BashOperator(
        task_id="sync_to_s3",
        bash_command=f"bash {PROJECT_DIR}/phase4/sync_to_s3.sh ",
    )
    
    # --- Task 3: which day should glue curate? ---
    # the date param if given manual backfill, otherwise the day before the run
    # returned values are passed to later tasks through XCom
    @task
    def target_date(**context):
        if context["params"]["date"]:
            return context["params"]["date"]
        run_time = context["logical_date"] or context["dag_run"].run_after
        return (run_time - timedelta(days=1)).strftime("%Y-%m-%d")
    
    # --- Task 4: run the glue job for that day and wait for it to finish ---
    # aws_conn_id=None = use the standard AWS connection 
    # ~/.aws of the pipeline-vm user). The {{ ... }} is a template filled in at
    # run time with target_date's result.
    
    curate = GlueJobOperator(
        task_id="curate_day",
        job_name="curate-daily",
        script_args={"--DATE": "{{ ti.xcom_pull(task_ids='target_date') }}"},
        aws_conn_id=None,
        region_name="us-west-2",
        wait_for_completion=True,
    )
    
    # --- Order ---
    # Freshness, then sync to s3, then Glue, then curate
    check_freshness() >> sync_to_s3 >> curate
    target_date() >> curate
    
daily_lake_maintenance()
