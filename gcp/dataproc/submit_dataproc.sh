#!/usr/bin/env bash
# Runs the check job on Dataproc Serverless (batches). Not run by the author: needs a GCP
# project with billing, the Dataproc API enabled and a GCS bucket.
#   PROJECT, REGION, BUCKET  - your values
#   RUNTIME_VERSION          - a Dataproc Serverless runtime with Spark 4 / Scala 2.13
#                              (check the Dataproc runtime release notes before use)
set -euo pipefail
: "${PROJECT:?set PROJECT}" "${REGION:?set REGION}" "${BUCKET:?set BUCKET}" "${RUNTIME_VERSION:?set RUNTIME_VERSION}"
RUN_DATE="${RUN_DATE:-2027-02-02}"
JAR=target/spark-data-quality-monitor.jar

gsutil cp "$JAR" "gs://$BUCKET/jobs/"
gsutil -m cp -r "out/data/flights_$RUN_DATE" "gs://$BUCKET/data/"

gcloud dataproc batches submit spark \
  --project="$PROJECT" --region="$REGION" --version="$RUNTIME_VERSION" \
  --jars="gs://$BUCKET/jobs/spark-data-quality-monitor.jar" \
  --class=dq.Main \
  --properties=spark.sql.adaptive.enabled=true \
  -- check --input "gs://$BUCKET/data/flights_$RUN_DATE" --format parquet \
     --dataset flights --run-date "$RUN_DATE" --out /tmp/dq-out
# Note: --out is a local path on the driver. To keep the run JSON and dashboard,
# copy them to GCS at the end of the job or write them to BigQuery (see ../bigquery).
