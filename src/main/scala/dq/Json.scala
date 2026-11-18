package dq

import java.util.Locale

/** Small JSON writer, so the job needs nothing beyond the Spark jars. */
object Json {

  def esc(s: String): String = {
    val sb = new StringBuilder
    s.foreach {
      case '"'          => sb.append("\\\"")
      case '\\'         => sb.append("\\\\")
      case '\n'         => sb.append("\\n")
      case '\r'         => sb.append("\\r")
      case '\t'         => sb.append("\\t")
      case c if c < ' ' => sb.append(f"\\u${c.toInt}%04x")
      case c            => sb.append(c)
    }
    sb.toString
  }

  def str(s: String): String = "\"" + esc(s) + "\""

  def num(d: Double): String = String.format(Locale.ROOT, "%.6f", Double.box(d))

  def check(c: CheckResult): String =
    s"""{"name":${str(c.name)},"column":${str(c.column)},"failed_rows":${c.failedRows},""" +
      s""""total_rows":${c.totalRows},"ratio":${num(c.ratio)},"status":${str(c.status.name)},""" +
      s""""detail":${str(c.detail)},"sample_ids":[${c.sampleIds.map(str).mkString(",")}]}"""

  def run(r: RunResult): String =
    s"""{"run_id":${str(r.runId)},"dataset":${str(r.dataset)},"run_date":${str(r.runDate)},""" +
      s""""rows":${r.rows},"duration_ms":${r.durationMs},"status":${str(r.status.name)},""" +
      s""""checks":[${r.checks.map(check).mkString(",")}]}"""
}
