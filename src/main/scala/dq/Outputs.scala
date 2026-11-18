package dq

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}
import java.time.Instant
import scala.jdk.CollectionConverters._

/** Run history, monitoring dashboard and the Bigtable-ready metrics export. */
object Outputs {

  private val HistoryHeader = "run_id,dataset,run_date,rows,status,duration_ms,problems"

  def readHistory(outDir: Path): Seq[HistoryRow] = {
    val f = outDir.resolve("history.csv")
    if (!Files.exists(f)) Seq.empty
    else Files.readAllLines(f, StandardCharsets.UTF_8).asScala.toSeq.drop(1).filter(_.nonEmpty).map { l =>
      val p = l.split(",", -1)
      HistoryRow(p(0), p(1), p(2), p(3).toLong, Status.parse(p(4)), p(5).toLong, p(6).toInt)
    }
  }

  def appendHistory(outDir: Path, r: RunResult): Unit = {
    val f = outDir.resolve("history.csv")
    if (!Files.exists(f)) Files.write(f, (HistoryHeader + "\n").getBytes(StandardCharsets.UTF_8))
    val line = Seq(r.runId, r.dataset, r.runDate, r.rows, r.status.name, r.durationMs, r.problems.size).mkString(",")
    Files.write(f, (line + "\n").getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND)
  }

  def writeText(path: Path, text: String): Unit = {
    Files.createDirectories(path.getParent)
    Files.write(path, text.getBytes(StandardCharsets.UTF_8))
  }

  // ---------- Bigtable ----------

  /** Reverse timestamp, zero padded so newer runs sort first. */
  def reverseTs(epochMillis: Long): String = f"${Long.MaxValue - epochMillis}%019d"

  def bigtableRowKey(dataset: String, check: String, epochMillis: Long): String =
    s"$dataset#$check#${reverseTs(epochMillis)}"

  /**
   * CSV in the layout used by `cbt import`: first header row = column families,
   * second header row = column qualifiers, first column = row key.
   */
  def bigtableCsv(r: RunResult, epochMillis: Long): String = {
    val header1 = ",m,m,m,r,r"
    val header2 = "rowkey,status,failed_rows,ratio,run_id,rows"
    val rows = r.checks.map { c =>
      Seq(bigtableRowKey(r.dataset, c.name, epochMillis), c.status.name, c.failedRows.toString,
        Json.num(c.ratio), r.runId, r.rows.toString).mkString(",")
    }
    (Seq(header1, header2) ++ rows).mkString("\n") + "\n"
  }

  // ---------- dashboard ----------

  private def h(s: String): String =
    s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;")

  private def badge(s: Status): String = s"""<span class="b ${s.name}">${s.name}</span>"""

  def dashboard(latest: RunResult, history: Seq[HistoryRow]): String = {
    val checkRows = latest.checks.map { c =>
      f"""<tr><td>${h(c.name)}</td><td>${badge(c.status)}</td><td class="n">${c.failedRows}%d</td>""" +
        f"""<td class="n">${c.ratio * 100}%.2f %%</td><td>${h(c.detail)}</td>""" +
        s"""<td class="ids">${h(c.sampleIds.mkString(" "))}</td></tr>"""
    }.mkString("\n")

    val hist = history.filter(_.dataset == latest.dataset).takeRight(14)
    val maxRows = if (hist.isEmpty) 1L else math.max(1L, hist.map(_.rows).max)
    val barW = 34
    val bars = hist.zipWithIndex.map { case (x, i) =>
      val bh = math.max(2, (x.rows.toDouble / maxRows * 120).toInt)
      val xPos = 10 + i * (barW + 8)
      s"""<rect class="${x.status.name}" x="$xPos" y="${130 - bh}" width="$barW" height="$bh"><title>${h(x.runId)}: ${x.rows} rows, ${x.status.name}</title></rect>""" +
        s"""<text x="${xPos + barW / 2}" y="146" text-anchor="middle">${h(x.runDate.takeRight(5))}</text>"""
    }.mkString
    val svgW = math.max(300, 20 + hist.size * (barW + 8))

    val runRows = history.reverse.take(20).map { x =>
      s"""<tr><td>${h(x.runId)}</td><td>${h(x.dataset)}</td><td>${h(x.runDate)}</td><td class="n">${x.rows}</td>""" +
        s"""<td>${badge(x.status)}</td><td class="n">${x.problems}</td><td class="n">${x.durationMs} ms</td></tr>"""
    }.mkString("\n")

    s"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Data quality monitor</title>
<style>
:root{--bg:#f7f7f5;--fg:#1d1d1b;--muted:#6b6b66;--line:#ddddd8;--ok:#2e7d4f;--warn:#b7791f;--fail:#c0392b;--card:#fff}
@media (prefers-color-scheme: dark){:root{--bg:#161615;--fg:#ececea;--muted:#a3a39e;--line:#33332f;--card:#1f1f1d}}
body{margin:0;font:14px/1.45 system-ui,sans-serif;background:var(--bg);color:var(--fg)}
main{max-width:1100px;margin:0 auto;padding:24px 16px}
h1{font-size:20px;margin:0 0 4px} h2{font-size:15px;margin:28px 0 8px}
.muted{color:var(--muted)} .card{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:16px;overflow-x:auto}
table{border-collapse:collapse;width:100%} th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--line);vertical-align:top}
th{font-weight:600;color:var(--muted)} .n{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap} .ids{font-family:ui-monospace,monospace;font-size:12px}
.b{display:inline-block;padding:1px 8px;border-radius:10px;font-size:12px;font-weight:600;color:#fff}
.b.OK{background:var(--ok)} .b.WARN{background:var(--warn)} .b.FAIL{background:var(--fail)}
rect.OK{fill:var(--ok)} rect.WARN{fill:var(--warn)} rect.FAIL{fill:var(--fail)} svg text{fill:var(--muted);font-size:11px}
.big{font-size:28px;font-weight:700}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px}
</style></head><body><main>
<h1>Data quality monitor</h1>
<p class="muted">Synthetic data. Generated ${h(Instant.now().toString)} by spark-data-quality-monitor.</p>
<div class="grid">
<div class="card"><div class="muted">Dataset</div><div class="big">${h(latest.dataset)}</div></div>
<div class="card"><div class="muted">Latest run</div><div class="big">${h(latest.runDate)}</div></div>
<div class="card"><div class="muted">Status</div><div class="big">${badge(latest.status)}</div></div>
<div class="card"><div class="muted">Rows</div><div class="big">${latest.rows}</div></div>
<div class="card"><div class="muted">Checks needing attention</div><div class="big">${latest.problems.size} / ${latest.checks.size}</div></div>
</div>
<h2>Checks in the latest run</h2>
<div class="card"><table><thead><tr><th>Check</th><th>Status</th><th class="n">Failed rows</th><th class="n">Ratio</th><th>Rule</th><th>Sample flight_id</th></tr></thead>
<tbody>
$checkRows
</tbody></table></div>
<h2>Rows per run</h2>
<div class="card"><svg width="$svgW" height="152" role="img" aria-label="Rows per run">$bars</svg></div>
<h2>Run history</h2>
<div class="card"><table><thead><tr><th>Run</th><th>Dataset</th><th>Date</th><th class="n">Rows</th><th>Status</th><th class="n">Problems</th><th class="n">Duration</th></tr></thead>
<tbody>
$runRows
</tbody></table></div>
</main></body></html>
"""
  }
}
