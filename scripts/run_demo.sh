#!/usr/bin/env bash
# Full local demo: tests, two daily batches (clean, then with injected defects), dashboard,
# notifications (dry run unless TEAMS_WEBHOOK_URL is set) and the CSV vs Parquet benchmark.
set -uo pipefail
cd "$(dirname "$0")/.."
SPARK_HOME="${SPARK_HOME:-$(python3 -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')}"
SUBMIT="$SPARK_HOME/bin/spark-submit --master local[*] --driver-java-options -Dlog4j2.configurationFile=conf/log4j2.properties --class dq.Main target/spark-data-quality-monitor.jar"
ROWS="${ROWS:-200000}"
BENCH_ROWS="${BENCH_ROWS:-2000000}"
rm -rf out && mkdir -p out evidence

$SUBMIT test | tee evidence/tests.txt
$SUBMIT generate --rows "$ROWS" --date 2027-02-01 --out out/data/flights_2027-02-01
$SUBMIT generate --rows "$((ROWS * 70 / 100))" --date 2027-02-02 --defects --out out/data/flights_2027-02-02

$SUBMIT check --input out/data/flights_2027-02-01 --dataset flights --run-date 2027-02-01 --out out | tee evidence/run_day1.txt
echo "exit code day 1: ${PIPESTATUS[0]}" | tee -a evidence/run_day1.txt
$SUBMIT check --input out/data/flights_2027-02-02 --dataset flights --run-date 2027-02-02 --out out | tee evidence/run_day2.txt
echo "exit code day 2: ${PIPESTATUS[0]}" | tee -a evidence/run_day2.txt

$SUBMIT benchmark --rows "$BENCH_ROWS" --out out/benchmark > /dev/null
cp out/benchmark/benchmark.md out/benchmark/plans.txt evidence/
cp out/dashboard.html evidence/dashboard.html
cp out/notifications/teams_*.json evidence/teams_payload_example.json
cp out/notifications/email_*.eml evidence/email_example.eml
cp out/bigtable/flights-2027-02-02-*.csv evidence/bigtable_dq_metrics_example.csv
cp out/runs/flights-2027-02-02-*.json evidence/run_day2.json
echo "done: see evidence/"
