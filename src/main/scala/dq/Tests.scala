package dq

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.SparkSession

/** Plain test runner (no test framework needed beyond the Spark jars). */
object Tests {

  private var passed = 0
  private var failed = 0

  private def t(name: String)(body: => Unit): Unit =
    try { body; passed += 1; println(s"PASS  $name") }
    catch { case e: Throwable => failed += 1; println(s"FAIL  $name: ${e.getMessage}") }

  private def eq[A](actual: A, expected: A, what: String): Unit =
    if (actual != expected) throw new AssertionError(s"$what: expected $expected, got $actual")

  def run(spark: SparkSession): Int = {
    val mapper = new ObjectMapper()
    val date = "2027-02-01"
    val clean = Generator.generate(spark, 4000, date, 7L, defects = false, partitions = 2).cache()
    val dirty = Generator.generate(spark, 4000, date, 7L, defects = true, partitions = 2).cache()
    val (cleanRows, cleanChecks) = Checks.all(clean, date, None)
    val (dirtyRows, dirtyChecks) = Checks.all(dirty, date, Some(4000L))
    def st(cs: Seq[CheckResult], n: String): CheckResult = cs.find(_.name == n).get

    t("clean data: every check is OK") {
      eq(cleanRows, 4000L, "rows")
      val bad = cleanChecks.filter(_.status != Ok).map(_.name)
      eq(bad, Seq.empty[String], "checks not OK")
    }
    t("generator is repeatable with the same seed") {
      val again = Generator.generate(spark, 4000, date, 7L, defects = false, partitions = 2)
      eq(again.exceptAll(clean).count(), 0L, "rows that differ")
    }
    t("missing actual_dep: 80 rows (2 %) -> FAIL") {
      val c = st(dirtyChecks, "not_null_actual_dep"); eq(c.failedRows, 80L, "failed rows"); eq(c.status, Fail, "status")
    }
    t("duplicated flight_id: 20 pairs -> 40 rows, FAIL") {
      val c = st(dirtyChecks, "unique_flight_id"); eq(c.failedRows, 40L, "failed rows"); eq(c.status, Fail, "status")
    }
    t("passengers above capacity: 14 rows (0.35 %) -> WARN") {
      val c = st(dirtyChecks, "passengers_within_capacity"); eq(c.failedRows, 14L, "failed rows"); eq(c.status, Warn, "status")
    }
    t("unknown origin ZZZ: 10 rows -> FAIL") {
      val c = st(dirtyChecks, "allowed_origin"); eq(c.failedRows, 10L, "failed rows"); eq(c.status, Fail, "status")
    }
    t("arrival before departure: 8 rows -> FAIL") {
      val c = st(dirtyChecks, "arrival_after_departure"); eq(c.failedRows, 8L, "failed rows"); eq(c.status, Fail, "status")
    }
    t("samples point at real failing rows") {
      val c = st(dirtyChecks, "allowed_origin")
      eq(c.sampleIds.nonEmpty, true, "has samples")
      val ids = c.sampleIds
      eq(dirty.filter(dirty("flight_id").isin(ids: _*) && dirty("origin") === "ZZZ").count() >= 1, true, "sample has ZZZ")
    }
    t("row count change thresholds: 5 % OK, 12 % WARN, 30 % FAIL, first run OK") {
      eq(Checks.rowCountChange(950, Some(1000)).status, Ok, "5 %")
      eq(Checks.rowCountChange(880, Some(1000)).status, Warn, "12 %")
      eq(Checks.rowCountChange(700, Some(1000)).status, Fail, "30 %")
      eq(Checks.rowCountChange(700, None).status, Ok, "no history")
    }
    t("freshness fails when the batch is older than the run date") {
      eq(Checks.freshness(clean, cleanRows, "2027-02-02").status, Fail, "status")
    }
    t("run JSON is valid and keeps the overall status") {
      val r = RunResult("r1", "flights", date, dirtyRows, 10, dirtyChecks)
      val node = mapper.readTree(Json.run(r))
      eq(node.get("status").asText(), "FAIL", "status")
      eq(node.get("checks").size(), dirtyChecks.size, "checks")
    }
    t("JSON escaping handles quotes, backslashes and newlines") {
      val s = "a \"quoted\" \\ path\nnext"
      eq(mapper.readTree(Json.str(s)).asText(), s, "round trip")
    }
    t("Teams payload is a valid Adaptive Card message listing only problem checks") {
      val r = RunResult("r1", "flights", date, dirtyRows, 10, dirtyChecks)
      val node = mapper.readTree(Notify.teamsPayload(r))
      eq(node.get("type").asText(), "message", "type")
      val card = node.get("attachments").get(0).get("content")
      eq(card.get("type").asText(), "AdaptiveCard", "card type")
      eq(card.get("body").get(2).get("facts").size(), r.problems.size, "facts")
    }
    t("email has a subject with the status and lists the problems") {
      val r = RunResult("r1", "flights", date, dirtyRows, 10, dirtyChecks)
      val e = Notify.email(r, "to@example.invalid", "from@example.invalid")
      eq(e.contains("Subject: [FAIL] Data quality flights 2027-02-01"), true, "subject")
      eq(e.contains("unique_flight_id"), true, "lists unique check")
    }
    t("Bigtable row keys sort newest run first") {
      val older = Outputs.bigtableRowKey("flights", "unique_flight_id", 1000L)
      val newer = Outputs.bigtableRowKey("flights", "unique_flight_id", 2000L)
      eq(newer < older, true, "newer sorts first")
      eq(older.split("#").last.length, 19, "padded length")
    }
    t("Bigtable CSV has family and qualifier header rows plus one row per check") {
      val r = RunResult("r1", "flights", date, dirtyRows, 10, dirtyChecks)
      val lines = Outputs.bigtableCsv(r, 1000L).trim.split("\n")
      eq(lines(0), ",m,m,m,r,r", "family row")
      eq(lines(1).startsWith("rowkey,"), true, "qualifier row")
      eq(lines.length, 2 + dirtyChecks.size, "line count")
    }

    println(s"\n$passed passed, $failed failed")
    spark.stop()
    if (failed == 0) 0 else 1
  }
}
