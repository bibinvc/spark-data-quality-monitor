package dq

sealed trait Status { def name: String; def rank: Int }
case object Ok extends Status { val name = "OK"; val rank = 0 }
case object Warn extends Status { val name = "WARN"; val rank = 1 }
case object Fail extends Status { val name = "FAIL"; val rank = 2 }

object Status {
  def parse(s: String): Status = s match {
    case "FAIL" => Fail
    case "WARN" => Warn
    case _      => Ok
  }
}

final case class CheckResult(
    name: String,
    column: String,
    failedRows: Long,
    totalRows: Long,
    status: Status,
    detail: String,
    sampleIds: Seq[String]
) {
  def ratio: Double = if (totalRows == 0) 0.0 else failedRows.toDouble / totalRows
}

final case class RunResult(
    runId: String,
    dataset: String,
    runDate: String,
    rows: Long,
    durationMs: Long,
    checks: Seq[CheckResult]
) {
  def status: Status = if (checks.isEmpty) Ok else checks.maxBy(_.status.rank).status
  def problems: Seq[CheckResult] = checks.filter(_.status != Ok)
}

/** One line of out/history.csv, used for the row-count check and the dashboard. */
final case class HistoryRow(
    runId: String,
    dataset: String,
    runDate: String,
    rows: Long,
    status: Status,
    durationMs: Long,
    problems: Int
)
