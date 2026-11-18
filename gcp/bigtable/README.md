# Bigtable layout for data quality metrics

Each check result of each run becomes one row, so a dashboard can read the latest status
of a check with a single prefix scan.

| Part | Value |
|---|---|
| Table | `dq_metrics` |
| Row key | `<dataset>#<check>#<reverse timestamp>` |
| Column family `m` | `status`, `failed_rows`, `ratio` |
| Column family `r` | `run_id`, `rows` |

The reverse timestamp is `Long.MaxValue - epoch millis`, padded to 19 digits, so the newest
run of a check sorts first. Reading `flights#unique_flight_id#` with a limit of 1 returns the
latest result. Keys start with the dataset and check name, not the time, so writes from
many runs spread across the key space instead of hitting one node.

The check job writes `out/bigtable/<run_id>.csv` in the layout used by `cbt import`
(family header row, qualifier header row, row key first).

```sh
# Not run by the author: needs a Bigtable instance (or the Bigtable emulator).
cbt -project "$PROJECT" -instance "$INSTANCE" createtable dq_metrics
cbt -project "$PROJECT" -instance "$INSTANCE" createfamily dq_metrics m
cbt -project "$PROJECT" -instance "$INSTANCE" createfamily dq_metrics r
cbt -project "$PROJECT" -instance "$INSTANCE" import dq_metrics out/bigtable/<run_id>.csv
cbt -project "$PROJECT" -instance "$INSTANCE" read dq_metrics prefix=flights#unique_flight_id# count=1
```
