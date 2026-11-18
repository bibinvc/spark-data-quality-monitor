package dq

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.time.Duration

/**
 * Builds a Microsoft Teams message (Adaptive Card, for a Workflows webhook) and an email
 * (.eml) when a run ends in WARN or FAIL. The Teams message is posted only when
 * TEAMS_WEBHOOK_URL is set; otherwise both messages are written to disk (dry run).
 * scripts/send_email.py sends the .eml through SMTP.
 */
object Notify {

  def summaryLines(r: RunResult): Seq[String] =
    r.problems.map(c => f"${c.status.name}%s  ${c.name}%s: ${c.failedRows}%d rows (${c.ratio * 100}%.2f %%). ${c.detail}%s")

  def teamsPayload(r: RunResult): String = {
    val facts = r.problems.map { c =>
      s"""{"title":${Json.str(c.status.name + " " + c.name)},"value":${Json.str(
        f"${c.failedRows}%d rows (${c.ratio * 100}%.2f %%) - ${c.detail}%s")}}"""
    }.mkString(",")
    val card =
      s"""{"type":"AdaptiveCard","$$schema":"http://adaptivecards.io/schemas/adaptive-card.json","version":"1.4",""" +
        s""""body":[{"type":"TextBlock","size":"Medium","weight":"Bolder","text":${Json.str(
          s"Data quality ${r.status.name}: ${r.dataset} ${r.runDate}")}},""" +
        s"""{"type":"TextBlock","wrap":true,"text":${Json.str(
          s"Run ${r.runId}, ${r.rows} rows, ${r.problems.size} check(s) need attention.")}},""" +
        s"""{"type":"FactSet","facts":[$facts]}]}"""
    s"""{"type":"message","attachments":[{"contentType":"application/vnd.microsoft.card.adaptive","content":$card}]}"""
  }

  def email(r: RunResult, to: String, from: String): String = {
    val body = (Seq(
      s"Dataset: ${r.dataset}",
      s"Run date: ${r.runDate}",
      s"Run id: ${r.runId}",
      s"Rows: ${r.rows}",
      s"Overall status: ${r.status.name}",
      "",
      "Checks that need attention:"
    ) ++ summaryLines(r) ++ Seq("", "Sent by spark-data-quality-monitor.")).mkString("\r\n")
    Seq(
      s"From: $from",
      s"To: $to",
      s"Subject: [${r.status.name}] Data quality ${r.dataset} ${r.runDate}",
      "MIME-Version: 1.0",
      "Content-Type: text/plain; charset=UTF-8",
      "",
      body
    ).mkString("\r\n")
  }

  /** Returns a short line describing what was done, for the job log. */
  def send(r: RunResult, outDir: Path): String = {
    if (r.status == Ok) return "status OK: no notification"
    val dir = outDir.resolve("notifications")
    Files.createDirectories(dir)
    val payload = teamsPayload(r)
    Files.write(dir.resolve(s"teams_${r.runId}.json"), payload.getBytes(StandardCharsets.UTF_8))
    val to = sys.env.getOrElse("MAIL_TO", "data-team@example.invalid")
    val from = sys.env.getOrElse("MAIL_FROM", "dq-monitor@example.invalid")
    Files.write(dir.resolve(s"email_${r.runId}.eml"), email(r, to, from).getBytes(StandardCharsets.UTF_8))

    sys.env.get("TEAMS_WEBHOOK_URL").filter(_.startsWith("https://")) match {
      case Some(url) =>
        val client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()
        val req = HttpRequest.newBuilder(URI.create(url))
          .timeout(Duration.ofSeconds(15))
          .header("Content-Type", "application/json")
          .POST(HttpRequest.BodyPublishers.ofString(payload))
          .build()
        val resp = client.send(req, HttpResponse.BodyHandlers.ofString())
        s"Teams webhook answered HTTP ${resp.statusCode()}; email written to $dir"
      case None =>
        s"dry run: Teams payload and email written to $dir (set TEAMS_WEBHOOK_URL to post)"
    }
  }
}
