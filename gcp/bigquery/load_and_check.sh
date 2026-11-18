#!/usr/bin/env bash
# Loads one Parquet batch into BigQuery and runs the same checks in SQL.
# Not run by the author. Works in the free BigQuery sandbox (no billing) if the Parquet
# files are uploaded to a GCS bucket you can read, or loaded from a local file with bq load.
#   PROJECT, DATASET (BigQuery dataset), SOURCE (gs://... or local Parquet file)
set -euo pipefail
: "${PROJECT:?set PROJECT}" "${DATASET:?set DATASET}" "${SOURCE:?set SOURCE}"
RUN_DATE="${RUN_DATE:-2027-02-02}"
TABLE="$PROJECT.$DATASET.flights"

bq --project_id="$PROJECT" load --replace --source_format=PARQUET "$DATASET.flights" "$SOURCE"
sed -e "s/{{TABLE}}/$TABLE/g" -e "s/{{RUN_DATE}}/$RUN_DATE/g" "$(dirname "$0")/checks.sql" \
  | bq --project_id="$PROJECT" query --use_legacy_sql=false --format=prettyjson
