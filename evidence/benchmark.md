# Benchmark: CSV vs Parquet

Synthetic flights, 2000000 rows, Spark 4.0.1, local[2] on one machine. Median of 3 runs after one warm-up. The CSV reader gets an explicit schema, so schema inference is not counted.

## Size on disk

| Format | Size |
|---|---:|
| CSV | 268.8 MB |
| Parquet (snappy) | 23.9 MB |

## Query time

| Query | CSV | Parquet | CSV time / Parquet time |
|---|---:|---:|---:|
| q1 avg delay per origin (all rows, group by) | 1472 ms | 356 ms | 4.1 |
| q2 load factor (2 of 10 columns) | 1169 ms | 228 ms | 5.1 |
| q3 AMS departures delayed > 60 min (filter) | 1018 ms | 266 ms | 3.8 |

## Shuffle partitions and adaptive query execution

Group by origin and destination on Parquet.

| AQE | spark.sql.shuffle.partitions | Median |
|---|---:|---:|
| false | 200 | 1220 ms |
| false | 8 | 353 ms |
| true | 200 | 472 ms |

## Memory use of cached data

Spark in-memory cache of the Parquet data, read from the storage info after count().

| Cached columns | Memory |
|---|---:|
| all 10 columns | 77.3 MB |
| origin, delay_min, passengers only | 8.6 MB |

Physical plans for q2 and q3 are in plans.txt (ReadSchema shows the pruned columns, PushedFilters the filters pushed to Parquet).
