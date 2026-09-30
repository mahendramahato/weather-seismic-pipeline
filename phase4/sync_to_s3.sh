#!/usr/bin/env bash
# Uploads new/changed files from the local streaming lake to the S3 raw zone.
# Safe to run repeatedly: sync only uploads what's new or changed.

# Stop at the first error (-e), treat unset variables as errors (-u),
# and fail a pipeline if any command in it fails (-o pipefail).
set -euo pipefail

BUCKET="weather-seismic-lake-mahendra"
LAKE_DIR="$(dirname "$0")/../data/lake"

# Files that must not be uploaded: local checksums, empty markers,
# half-written batches, and Spark's local file list (holds local paths).
EXCLUDES=(
  --exclude "*.crc"
  --exclude "*_SUCCESS"
  --exclude "*_temporary/*"
  --exclude "*_spark_metadata/*"
)

for dataset in weather seismic; do
  echo "syncing $dataset..."
  aws s3 sync "$LAKE_DIR/$dataset" "s3://$BUCKET/raw/$dataset" "${EXCLUDES[@]}"
done

echo "done"
