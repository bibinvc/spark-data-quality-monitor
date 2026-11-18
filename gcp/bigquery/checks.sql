-- Same rules as src/main/scala/dq/Checks.scala, in BigQuery Standard SQL.
-- {{TABLE}} and {{RUN_DATE}} are replaced by load_and_check.sh.
WITH t AS (SELECT * FROM `{{TABLE}}`),
total AS (SELECT COUNT(*) AS n FROM t),
results AS (
  SELECT 'unique_flight_id' AS check_name,
         (SELECT COALESCE(SUM(c), 0) FROM (SELECT COUNT(*) AS c FROM t GROUP BY flight_id HAVING COUNT(*) > 1)) AS failed_rows
  UNION ALL SELECT 'not_null_actual_dep', COUNTIF(actual_dep IS NULL) FROM t
  UNION ALL SELECT 'not_null_actual_arr', COUNTIF(actual_arr IS NULL) FROM t
  UNION ALL SELECT 'allowed_origin',
         COUNTIF(origin NOT IN ('AMS','CDG','LHR','FRA','MAD','BCN','FCO','CPH','OSL','DUB','LIS','VIE')) FROM t
  UNION ALL SELECT 'passengers_within_capacity', COUNTIF(passengers < 0 OR passengers > capacity) FROM t
  UNION ALL SELECT 'arrival_after_departure', COUNTIF(actual_arr <= actual_dep) FROM t
  UNION ALL SELECT 'delay_in_range', COUNTIF(delay_min < -60 OR delay_min > 1440) FROM t
  UNION ALL SELECT 'freshness_flight_date',
         IF(MAX(flight_date) = DATE '{{RUN_DATE}}', 0, COUNT(*)) FROM t
)
SELECT r.check_name, r.failed_rows, total.n AS total_rows,
       ROUND(SAFE_DIVIDE(r.failed_rows, total.n), 6) AS ratio
FROM results r CROSS JOIN total
ORDER BY r.failed_rows DESC, r.check_name;
