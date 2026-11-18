package dq

import java.nio.file.{Files, Path, Paths}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import scala.jdk.CollectionConverters._

/**
 * Compares CSV and Parquet on the same synthetic data, and the effect of shuffle partitions
 * and adaptive query execution. Each query reads the files again and runs 3 times after one
 * warm-up run; the median is reported. Results depend on the machine.
 */
object Benchmark {

  private def dirSize(p: Path): Long =
    if (!Files.exists(p)) 0L
    else Files.walk(p).iterator().asScala.filter(Files.isRegularFile(_))
      .filter(f => !f.getFileName.toString.startsWith(".") && !f.getFileName.toString.startsWith("_"))
      .map(Files.size).sum

  private def timeMs(f: => Any): Double = {
    val t0 = System.nanoTime(); f; (System.nanoTime() - t0) / 1e6
  }

  private def median(xs: Seq[Double]): Double = { val s = xs.sorted; s(s.size / 2) }

  final case class Row(label: String, format: String, medianMs: Double)

  def run(spark: SparkSession, rows: Long, outDir: Path): String = {
    val dataDir = outDir.resolve("bench-data")
    val csvPath = dataDir.resolve("csv").toString
    val pqPath = dataDir.resolve("parquet").toString

    val df = Generator.generate(spark, rows, "2027-02-01", 42L, defects = false)
    df.write.mode("overwrite").option("header", "true").csv(csvPath)
    df.write.mode("overwrite").option("compression", "snappy").parquet(pqPath)

    def read(format: String): DataFrame =
      if (format == "csv") spark.read.option("header", "true").schema(Generator.schema).csv(csvPath)
      else spark.read.parquet(pqPath)

    val queries: Seq[(String, DataFrame => Any)] = Seq(
      "q1 avg delay per origin (all rows, group by)" ->
        ((d: DataFrame) => d.groupBy("origin").agg(avg("delay_min"), count(lit(1))).collect()),
      "q2 load factor (2 of 10 columns)" ->
        ((d: DataFrame) => d.agg(sum("passengers") / sum("capacity")).collect()),
      "q3 AMS departures delayed > 60 min (filter)" ->
        ((d: DataFrame) => d.filter(col("origin") === "AMS" && col("delay_min") > 60).count())
    )

    val results = for {
      (label, q) <- queries
      format <- Seq("csv", "parquet")
    } yield {
      q(read(format))
      Row(label, format, median((1 to 3).map(_ => timeMs(q(read(format))))))
    }

    // shuffle partitions and AQE on the group-by query, Parquet input
    val shuffleCases = Seq(("false", "200"), ("false", "8"), ("true", "200"))
    val shuffle = shuffleCases.map { case (aqe, parts) =>
      spark.conf.set("spark.sql.adaptive.enabled", aqe)
      spark.conf.set("spark.sql.shuffle.partitions", parts)
      val q = (d: DataFrame) => d.groupBy("origin", "destination").agg(avg("delay_min")).collect()
      q(read("parquet"))
      (aqe, parts, median((1 to 3).map(_ => timeMs(q(read("parquet"))))))
    }
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", "200")

    // memory: cache every column vs only the 3 columns a delay report needs
    def cachedBytes(d: DataFrame): Long = {
      d.cache(); d.count()
      val bytes = spark.sparkContext.getRDDStorageInfo.map(_.memSize).sum
      d.unpersist(blocking = true)
      bytes
    }
    val memAll = cachedBytes(read("parquet"))
    val memSlim = cachedBytes(read("parquet").select("origin", "delay_min", "passengers"))

    val plan2 = read("parquet").agg(sum("passengers") / sum("capacity")).queryExecution.executedPlan.toString
    val plan3 = read("parquet").filter(col("origin") === "AMS" && col("delay_min") > 60)
      .queryExecution.executedPlan.toString
    Outputs.writeText(outDir.resolve("plans.txt"),
      s"q2 on Parquet (column pruning, see ReadSchema)\n$plan2\n\nq3 on Parquet (see PushedFilters)\n$plan3\n")

    val csvSize = dirSize(Paths.get(csvPath))
    val pqSize = dirSize(Paths.get(pqPath))
    val mb = (b: Long) => b / 1024.0 / 1024.0

    val sb = new StringBuilder
    sb.append(s"# Benchmark: CSV vs Parquet\n\n")
    sb.append(s"Synthetic flights, $rows rows, Spark ${spark.version}, local[${spark.sparkContext.defaultParallelism}] on one machine. ")
    sb.append("Median of 3 runs after one warm-up. The CSV reader gets an explicit schema, so schema inference is not counted.\n\n")
    sb.append("## Size on disk\n\n| Format | Size |\n|---|---:|\n")
    sb.append(f"| CSV | ${mb(csvSize)}%.1f MB |\n| Parquet (snappy) | ${mb(pqSize)}%.1f MB |\n\n")
    sb.append("## Query time\n\n| Query | CSV | Parquet | CSV time / Parquet time |\n|---|---:|---:|---:|\n")
    queries.map(_._1).foreach { label =>
      val c = results.find(r => r.label == label && r.format == "csv").get.medianMs
      val p = results.find(r => r.label == label && r.format == "parquet").get.medianMs
      sb.append(f"| $label | $c%.0f ms | $p%.0f ms | ${c / p}%.1f |\n")
    }
    sb.append("\n## Shuffle partitions and adaptive query execution\n\n")
    sb.append("Group by origin and destination on Parquet.\n\n| AQE | spark.sql.shuffle.partitions | Median |\n|---|---:|---:|\n")
    shuffle.foreach { case (aqe, parts, ms) => sb.append(f"| $aqe | $parts | $ms%.0f ms |\n") }
    sb.append("\n## Memory use of cached data\n\n")
    sb.append("Spark in-memory cache of the Parquet data, read from the storage info after count().\n\n")
    sb.append("| Cached columns | Memory |\n|---|---:|\n")
    sb.append(f"| all 10 columns | ${mb(memAll)}%.1f MB |\n")
    sb.append(f"| origin, delay_min, passengers only | ${mb(memSlim)}%.1f MB |\n")
    sb.append("\nPhysical plans for q2 and q3 are in plans.txt (ReadSchema shows the pruned columns, PushedFilters the filters pushed to Parquet).\n")
    val md = sb.toString
    Outputs.writeText(outDir.resolve("benchmark.md"), md)
    md
  }
}
