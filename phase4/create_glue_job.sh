#!/usr/bin/env bash
# Creates the curate-daily Glue ETL job. The job definition lives here, in git,
# so it can be recreated exactly if it's deleted or the account is rebuilt.
set -euo pipefail

BUCKET="weather-seismic-lake-mahendra"

# Upload the latest version of the job script first, so the job always runs
# the code that's in the repo.
aws s3 cp "$(dirname "$0")/curate_job.py" "s3://$BUCKET/scripts/curate_job.py"

# 2 x G.1X is Glue's minimum size; timeout 10 (minutes) caps the cost of a
# stuck run (the default is 48 hours); no automatic retries while learning.
aws glue create-job \
  --name curate-daily \
  --role AWSGlueServiceRole-weather-seismic \
  --command "{\"Name\": \"glueetl\", \"ScriptLocation\": \"s3://$BUCKET/scripts/curate_job.py\", \"PythonVersion\": \"3\"}" \
  --glue-version "5.0" \
  --worker-type G.1X \
  --number-of-workers 2 \
  --timeout 10 \
  --max-retries 0 \
  --default-arguments "{\"--BUCKET\": \"$BUCKET\", \"--enable-glue-datacatalog\": \"true\"}"

echo "created job curate-daily"
