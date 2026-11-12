package flightnetwork

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.spark.sql.SparkSession
import java.net.InetSocketAddress
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import scala.io.Source
import scala.util.control.NonFatal

object FlightWebApp {

  val DefaultSample: String =
    """source,destination,flights
DELHI,MUMBAI,120
DELHI,BENGALURU,95
MUMBAI,HYDERABAD,70""".stripMargin

  def escapeHtml(s: String): String = {
    if (s == null) ""
    else s.replace("&", "&amp;")
      .replace("<", "&lt;")
      .replace(">", "&gt;")
      .replace("\"", "&quot;")
      .replace("'", "&#39;")
  }

  def parseFormData(body: String): Map[String, String] = {
    body.split('&').flatMap { pair =>
      val idx = pair.indexOf('=')
      if (idx >= 0) {
        val key = URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8.name())
        val value = URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8.name())
        Some(key -> value)
      } else None
    }.toMap
  }

  def renderPage(
      inputRoutes: String,
      resultOpt: Option[(AnalysisSummary, String, Seq[String])] = None,
      errorOpt: Option[String] = None
  ): String = {
    val escapedInput = escapeHtml(inputRoutes)

    val resultsHtml = resultOpt match {
      case Some((summary, rawReport, warnings)) =>
        val maxDegree = if (summary.degrees.nonEmpty) summary.degrees.map(_._2).max else 1
        val maxPageRank = if (summary.pageRank.nonEmpty) summary.pageRank.map(_._2).max else 1.0

        val warningsHtml = if (warnings.nonEmpty) {
          s"""<div class="card card-warning" style="margin-top: 1.25rem;">
             |  <div class="warning-title">⚠️ Warnings (${warnings.length} row(s) skipped)</div>
             |  <ul class="warning-list">
             |    ${warnings.map(w => s"<li>${escapeHtml(w)}</li>").mkString("\n")}
             |  </ul>
             |</div>""".stripMargin
        } else ""

        val degreeRows = summary.degrees.zipWithIndex.map { case ((name, deg), idx) =>
          val pct = (deg.toDouble / maxDegree.toDouble) * 100.0
          val isTied = summary.degrees.count(_._2 == deg) > 1
          val tieTag = if (isTied) s"""<span class="tie-tag">Tied</span>""" else ""
          val barClass = if (isTied) "bar-fill bar-tie" else "bar-fill"
          val valColor = if (isTied) "color: var(--warning);" else ""

          s"""<tr>
             |  <td class="col-rank">${idx + 1}</td>
             |  <td class="col-name">${escapeHtml(name)} $tieTag</td>
             |  <td class="col-val" style="$valColor">$deg</td>
             |  <td class="col-bar">
             |    <div class="bar-bg"><div class="$barClass" style="width: ${f"$pct%.1f"}%%;"></div></div>
             |  </td>
             |</tr>""".stripMargin
        }.mkString("\n")

        val pageRankRows = summary.pageRank.zipWithIndex.map { case ((name, rank), idx) =>
          val pct = if (maxPageRank > 0) (rank / maxPageRank) * 100.0 else 0.0
          val isTied = summary.pageRank.count(p => math.abs(p._2 - rank) < 1e-6) > 1
          val tieTag = if (isTied) s"""<span class="tie-tag">Tied</span>""" else ""
          val barClass = if (isTied) "bar-fill bar-tie" else "bar-fill"
          val valColor = if (isTied) "color: var(--warning);" else ""

          s"""<tr>
             |  <td class="col-rank">${idx + 1}</td>
             |  <td class="col-name">${escapeHtml(name)} $tieTag</td>
             |  <td class="col-val" style="$valColor">${f"$rank%.6f"}</td>
             |  <td class="col-bar">
             |    <div class="bar-bg"><div class="$barClass" style="width: ${f"$pct%.1f"}%%;"></div></div>
             |  </td>
             |</tr>""".stripMargin
        }.mkString("\n")

        val componentsHtml = summary.components.zipWithIndex.map { case ((compId, members), idx) =>
          val membersStr = members.map(escapeHtml).mkString(", ")
          s"""<div class="component-item">
             |  <strong>Component ${idx + 1} (Representative ID $compId):</strong> [$membersStr]
             |</div>""".stripMargin
        }.mkString("\n")

        s"""$warningsHtml
           |
           |<div class="grid-summary" style="margin-top: 1.5rem;">
           |  <div class="card">
           |    <div class="stat-label">Airports</div>
           |    <div class="stat-val">${summary.vertexCount}</div>
           |    <div class="stat-sub">Vertices</div>
           |  </div>
           |  <div class="card">
           |    <div class="stat-label">Routes</div>
           |    <div class="stat-val">${summary.edgeCount}</div>
           |    <div class="stat-sub">Directed Edges</div>
           |  </div>
           |  <div class="card">
           |    <div class="stat-label">Components</div>
           |    <div class="stat-val">${summary.components.length}</div>
           |    <div class="stat-sub">Undirected</div>
           |  </div>
           |</div>
           |
           |<div class="card" style="margin-top: 1.25rem;">
           |  <div class="section-title">Degree Ranking (In + Out Degrees)</div>
           |  <table>
           |    <thead>
           |      <tr>
           |        <th class="col-rank">#</th>
           |        <th class="col-name">Airport</th>
           |        <th class="col-val">Degree</th>
           |        <th class="col-bar">Distribution (Max = $maxDegree)</th>
           |      </tr>
           |    </thead>
           |    <tbody>
           |      $degreeRows
           |    </tbody>
           |  </table>
           |</div>
           |
           |<div class="card" style="margin-top: 1.25rem;">
           |  <div class="section-title">PageRank Ranking (Tolerance = 0.0001)</div>
           |  <table>
           |    <thead>
           |      <tr>
           |        <th class="col-rank">#</th>
           |        <th class="col-name">Airport</th>
           |        <th class="col-val" style="width: 80px;">Score</th>
           |        <th class="col-bar">Relative Score</th>
           |      </tr>
           |    </thead>
           |    <tbody>
           |      $pageRankRows
           |    </tbody>
           |  </table>
           |  <div class="caption">Scores show relative importance, not probabilities or passenger traffic.</div>
           |</div>
           |
           |<div class="card" style="margin-top: 1.25rem;">
           |  <div class="section-title">Connected Components</div>
           |  <div class="component-box">
           |    $componentsHtml
           |  </div>
           |</div>
           |
           |<details style="margin-top: 1.25rem;">
           |  <summary>View Formatted Console Report</summary>
           |  <pre class="console-pre"><code>${escapeHtml(rawReport)}</code></pre>
           |</details>
           |""".stripMargin

      case None =>
        errorOpt match {
          case Some(err) =>
            s"""<div class="card card-error" style="margin-top: 1.5rem;">
               |  <div class="error-title">❌ Analysis Error</div>
               |  <p class="error-desc">${escapeHtml(err)}</p>
               |</div>""".stripMargin
          case None => ""
        }
    }

    s"""<!DOCTYPE html>
       |<html lang="en">
       |<head>
       |  <meta charset="UTF-8">
       |  <meta name="viewport" content="width=device-width, initial-scale=1.0">
       |  <title>Flight Network Analysis: Interactive Console</title>
       |  <script>
       |    (function() {
       |      try {
       |        var saved = localStorage.getItem('flight-theme');
       |        if (saved === 'light' || saved === 'dark') {
       |          document.documentElement.setAttribute('data-theme', saved);
       |        }
       |      } catch(e) {}
       |    })();
       |  </script>
       |  <style>
       |    :root {
       |      --bg: #0f1419;
       |      --card-bg: #1a2129;
       |      --card-inner-bg: rgba(255, 255, 255, 0.03);
       |      --text: #e6edf3;
       |      --text-muted: #8b98a5;
       |      --accent: #4cc9f0;
       |      --warning: #ffb703;
       |      --border: #283340;
       |      --console-bg: #090d10;
       |      --console-text: #c9d1d9;
       |      --bar-bg: rgba(255, 255, 255, 0.08);
       |      --warning-bg: rgba(255, 183, 3, 0.12);
       |      --warning-border: rgba(255, 183, 3, 0.4);
       |      --error-bg: rgba(248, 113, 113, 0.12);
       |      --error-border: rgba(248, 113, 113, 0.4);
       |      --error-text: #f87171;
       |      --btn-sec-bg: rgba(255, 255, 255, 0.06);
       |      --btn-sec-color: var(--text);
       |      --btn-sec-border: var(--border);
       |    }
       |    @media (prefers-color-scheme: light) {
       |      :root:not([data-theme="dark"]) {
       |        --bg: #f5f7fa;
       |        --card-bg: #ffffff;
       |        --card-inner-bg: rgba(0, 0, 0, 0.03);
       |        --text: #1b2430;
       |        --text-muted: #5b6675;
       |        --accent: #0a7ea4;
       |        --warning: #b26a00;
       |        --border: #d0d7de;
       |        --console-bg: #f6f8fa;
       |        --console-text: #24292f;
       |        --bar-bg: rgba(0, 0, 0, 0.06);
       |        --warning-bg: rgba(178, 106, 0, 0.1);
       |        --warning-border: rgba(178, 106, 0, 0.35);
       |        --error-bg: rgba(207, 34, 46, 0.1);
       |        --error-border: rgba(207, 34, 46, 0.35);
       |        --error-text: #cf222e;
       |        --btn-sec-bg: rgba(0, 0, 0, 0.05);
       |        --btn-sec-color: var(--text);
       |        --btn-sec-border: var(--border);
       |      }
       |    }
       |    [data-theme="light"] {
       |      --bg: #f5f7fa;
       |      --card-bg: #ffffff;
       |      --card-inner-bg: rgba(0, 0, 0, 0.03);
       |      --text: #1b2430;
       |      --text-muted: #5b6675;
       |      --accent: #0a7ea4;
       |      --warning: #b26a00;
       |      --border: #d0d7de;
       |      --console-bg: #f6f8fa;
       |      --console-text: #24292f;
       |      --bar-bg: rgba(0, 0, 0, 0.06);
       |      --warning-bg: rgba(178, 106, 0, 0.1);
       |      --warning-border: rgba(178, 106, 0, 0.35);
       |      --error-bg: rgba(207, 34, 46, 0.1);
       |      --error-border: rgba(207, 34, 46, 0.35);
       |      --error-text: #cf222e;
       |      --btn-sec-bg: rgba(0, 0, 0, 0.05);
       |      --btn-sec-color: var(--text);
       |      --btn-sec-border: var(--border);
       |    }
       |    [data-theme="dark"] {
       |      --bg: #0f1419;
       |      --card-bg: #1a2129;
       |      --card-inner-bg: rgba(255, 255, 255, 0.03);
       |      --text: #e6edf3;
       |      --text-muted: #8b98a5;
       |      --accent: #4cc9f0;
       |      --warning: #ffb703;
       |      --border: #283340;
       |      --console-bg: #090d10;
       |      --console-text: #c9d1d9;
       |      --bar-bg: rgba(255, 255, 255, 0.08);
       |      --warning-bg: rgba(255, 183, 3, 0.12);
       |      --warning-border: rgba(255, 183, 3, 0.4);
       |      --error-bg: rgba(248, 113, 113, 0.12);
       |      --error-border: rgba(248, 113, 113, 0.4);
       |      --error-text: #f87171;
       |      --btn-sec-bg: rgba(255, 255, 255, 0.06);
       |      --btn-sec-color: var(--text);
       |      --btn-sec-border: var(--border);
       |    }
       |    * { box-sizing: border-box; margin: 0; padding: 0; }
       |    html, body {
       |      background-color: var(--bg);
       |      color: var(--text);
       |      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
       |      line-height: 1.5;
       |      width: 100%;
       |      overflow-x: hidden;
       |      transition: background-color 0.2s ease, color 0.2s ease;
       |    }
       |    .container {
       |      width: 100%;
       |      max-width: 820px;
       |      margin: 0 auto;
       |      padding: 1.25rem 1rem;
       |    }
       |    header {
       |      margin-bottom: 1.5rem;
       |      border-bottom: 1px solid var(--border);
       |      padding-bottom: 0.75rem;
       |    }
       |    .header-top {
       |      display: flex;
       |      align-items: center;
       |      justify-content: space-between;
       |      gap: 0.5rem;
       |      flex-wrap: wrap;
       |    }
       |    h1 {
       |      font-size: 1.35rem;
       |      font-weight: 700;
       |      color: var(--text);
       |    }
       |    .btn-theme {
       |      padding: 0.35rem 0.85rem;
       |      font-size: 0.8rem;
       |      font-weight: 600;
       |      border-radius: 6px;
       |      cursor: pointer;
       |      border: 1px solid var(--btn-sec-border);
       |      background: var(--btn-sec-bg);
       |      color: var(--btn-sec-color);
       |      transition: all 0.2s ease;
       |      display: inline-flex;
       |      align-items: center;
       |      justify-content: center;
       |      min-width: 60px;
       |    }
       |    .btn-theme:hover {
       |      opacity: 0.85;
       |      border-color: var(--accent);
       |    }
       |    .card {
       |      background: var(--card-bg);
       |      border: 1px solid var(--border);
       |      border-radius: 8px;
       |      padding: 1rem;
       |      transition: background-color 0.2s ease, border-color 0.2s ease;
       |    }
       |    .card-warning {
       |      border-color: var(--warning-border);
       |      background: var(--warning-bg);
       |    }
       |    .warning-title {
       |      color: var(--warning);
       |      font-weight: 700;
       |      font-size: 0.9rem;
       |      margin-bottom: 0.4rem;
       |    }
       |    .warning-list {
       |      margin-left: 1.25rem;
       |      font-size: 0.8rem;
       |      color: var(--text);
       |      line-height: 1.4;
       |    }
       |    .card-error {
       |      border-color: var(--error-border);
       |      background: var(--error-bg);
       |    }
       |    .error-title {
       |      color: var(--error-text);
       |      font-weight: 700;
       |      font-size: 0.95rem;
       |      margin-bottom: 0.25rem;
       |    }
       |    .error-desc {
       |      font-size: 0.85rem;
       |      color: var(--text);
       |    }
       |    label {
       |      display: block;
       |      font-size: 0.8rem;
       |      font-weight: 600;
       |      text-transform: uppercase;
       |      letter-spacing: 0.05em;
       |      color: var(--text-muted);
       |      margin-bottom: 0.5rem;
       |    }
       |    textarea {
       |      width: 100%;
       |      height: 140px;
       |      background: var(--console-bg);
       |      color: var(--console-text);
       |      border: 1px solid var(--border);
       |      border-radius: 6px;
       |      padding: 0.75rem;
       |      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
       |      font-size: 0.85rem;
       |      resize: vertical;
       |      line-height: 1.4;
       |      transition: border-color 0.2s ease, background-color 0.2s ease;
       |    }
       |    textarea:focus {
       |      outline: none;
       |      border-color: var(--accent);
       |    }
       |    .btn-row {
       |      display: flex;
       |      gap: 0.75rem;
       |      margin-top: 0.75rem;
       |    }
       |    button {
       |      padding: 0.55rem 1.25rem;
       |      font-size: 0.85rem;
       |      font-weight: 600;
       |      border-radius: 6px;
       |      cursor: pointer;
       |      border: 1px solid transparent;
       |      transition: opacity 0.2s ease;
       |    }
       |    button:hover { opacity: 0.9; }
       |    .btn-primary {
       |      background: var(--accent);
       |      color: #ffffff;
       |    }
       |    [data-theme="dark"] .btn-primary {
       |      color: #090d10;
       |    }
       |    .btn-secondary {
       |      background: var(--btn-sec-bg);
       |      color: var(--btn-sec-color);
       |      border-color: var(--btn-sec-border);
       |    }
       |    .grid-summary {
       |      display: grid;
       |      grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
       |      gap: 0.75rem;
       |    }
       |    .stat-label {
       |      font-size: 0.75rem;
       |      text-transform: uppercase;
       |      color: var(--text-muted);
       |      margin-bottom: 0.2rem;
       |    }
       |    .stat-val {
       |      font-size: 1.65rem;
       |      font-weight: 700;
       |      color: var(--accent);
       |      line-height: 1.1;
       |    }
       |    .stat-sub {
       |      font-size: 0.75rem;
       |      color: var(--text-muted);
       |      margin-top: 0.2rem;
       |    }
       |    .section-title {
       |      font-size: 1.05rem;
       |      font-weight: 600;
       |      margin-bottom: 0.75rem;
       |      color: var(--text);
       |    }
       |    table {
       |      width: 100%;
       |      border-collapse: collapse;
       |      font-size: 0.85rem;
       |    }
       |    th, td {
       |      padding: 0.55rem 0.5rem;
       |      text-align: left;
       |      border-bottom: 1px solid var(--border);
       |    }
       |    th {
       |      color: var(--text-muted);
       |      font-size: 0.75rem;
       |      text-transform: uppercase;
       |      letter-spacing: 0.04em;
       |    }
       |    tr:last-child td { border-bottom: none; }
       |    .col-rank { width: 35px; color: var(--text-muted); font-weight: 600; }
       |    .col-name { width: 110px; font-weight: 600; word-break: break-word; color: var(--text); }
       |    .col-val {
       |      width: 65px;
       |      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
       |      font-weight: 600;
       |      text-align: right;
       |      padding-right: 0.5rem;
       |      color: var(--text);
       |    }
       |    .col-bar { min-width: 80px; }
       |    .bar-bg {
       |      background: var(--bar-bg);
       |      height: 14px;
       |      border-radius: 3px;
       |      overflow: hidden;
       |      width: 100%;
       |      display: flex;
       |    }
       |    .bar-fill {
       |      height: 100%;
       |      background: var(--accent);
       |      border-radius: 3px;
       |    }
       |    .bar-tie { background: var(--warning); }
       |    .tie-tag {
       |      display: inline-block;
       |      font-size: 0.65rem;
       |      color: var(--warning);
       |      border: 1px solid var(--warning-border);
       |      background: var(--warning-bg);
       |      padding: 0.1rem 0.35rem;
       |      border-radius: 4px;
       |      margin-left: 0.25rem;
       |      vertical-align: middle;
       |      font-weight: 600;
       |      text-transform: uppercase;
       |    }
       |    .caption {
       |      font-size: 0.75rem;
       |      color: var(--text-muted);
       |      margin-top: 0.75rem;
       |      font-style: italic;
       |    }
       |    .component-box {
       |      background: var(--console-bg);
       |      border: 1px solid var(--border);
       |      border-radius: 6px;
       |      padding: 0.75rem;
       |      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
       |      font-size: 0.8rem;
       |      color: var(--console-text);
       |      line-height: 1.5;
       |    }
       |    .component-item { margin-bottom: 0.35rem; }
       |    .component-item:last-child { margin-bottom: 0; }
       |    details {
       |      background: var(--card-bg);
       |      border: 1px solid var(--border);
       |      border-radius: 8px;
       |      overflow: hidden;
       |    }
       |    summary {
       |      padding: 0.75rem 1rem;
       |      cursor: pointer;
       |      font-size: 0.85rem;
       |      font-weight: 600;
       |      color: var(--text);
       |      user-select: none;
       |    }
       |    summary:hover { background: var(--card-inner-bg); }
       |    .console-pre {
       |      background: var(--console-bg);
       |      color: var(--console-text);
       |      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
       |      font-size: 0.72rem;
       |      line-height: 1.4;
       |      padding: 0.85rem;
       |      border-top: 1px solid var(--border);
       |      white-space: pre-wrap;
       |      word-break: break-all;
       |      max-height: 300px;
       |      overflow-y: auto;
       |    }
       |    footer {
       |      text-align: center;
       |      font-size: 0.75rem;
       |      color: var(--text-muted);
       |      padding: 1.25rem 0;
       |      border-top: 1px solid var(--border);
       |      margin-top: 1.5rem;
       |    }
       |    @media (max-width: 480px) {
       |      .container { padding: 1rem 0.65rem; }
       |      .card { padding: 0.75rem 0.65rem; }
       |      h1 { font-size: 1.15rem; }
       |      .col-bar { min-width: 65px; }
       |    }
       |  </style>
       |</head>
       |<body>
       |  <div class="container">
       |    <header>
       |      <div class="header-top">
       |        <h1>Flight Network Analysis: Interactive Console</h1>
       |        <button id="theme-toggle" class="btn-theme" type="button" onclick="toggleTheme()" aria-label="Toggle theme">Light</button>
       |      </div>
       |    </header>
       |
       |    <div class="card">
       |      <form action="/analyze" method="POST">
       |        <label for="routes">Paste Flight Routes (CSV format: source,destination,flights)</label>
       |        <textarea id="routes" name="routes" spellcheck="false">$escapedInput</textarea>
       |        <div class="btn-row">
       |          <button type="submit" class="btn-primary">Analyze</button>
       |          <button type="button" class="btn-secondary" onclick="loadSample()">Load sample</button>
       |        </div>
       |      </form>
       |    </div>
       |
       |    $resultsHtml
       |
       |    <footer>
       |      Synthetic illustrative data. Not real airline data. &bull; Apache Spark GraphX
       |    </footer>
       |  </div>
       |
       |  <script>
       |    function loadSample() {
       |      document.getElementById('routes').value = "source,destination,flights\\nDELHI,MUMBAI,120\\nDELHI,BENGALURU,95\\nMUMBAI,HYDERABAD,70";
       |    }
       |
       |    function getEffectiveTheme() {
       |      var attr = document.documentElement.getAttribute('data-theme');
       |      if (attr === 'light' || attr === 'dark') return attr;
       |      if (window.matchMedia && window.matchMedia('(prefers-color-scheme: light)').matches) {
       |        return 'light';
       |      }
       |      return 'dark';
       |    }
       |
       |    function updateThemeButton() {
       |      var btn = document.getElementById('theme-toggle');
       |      if (!btn) return;
       |      var current = getEffectiveTheme();
       |      btn.textContent = (current === 'light') ? 'Dark' : 'Light';
       |    }
       |
       |    function toggleTheme() {
       |      var current = getEffectiveTheme();
       |      var next = (current === 'light') ? 'dark' : 'light';
       |      document.documentElement.setAttribute('data-theme', next);
       |      try {
       |        localStorage.setItem('flight-theme', next);
       |      } catch(e) {}
       |      updateThemeButton();
       |    }
       |
       |    document.addEventListener('DOMContentLoaded', updateThemeButton);
       |    if (window.matchMedia) {
       |      window.matchMedia('(prefers-color-scheme: light)').addEventListener('change', function() {
       |        try {
       |          if (!localStorage.getItem('flight-theme')) {
       |            updateThemeButton();
       |          }
       |        } catch(e) {}
       |      });
       |    }
       |  </script>
       |</body>
       |</html>""".stripMargin
  }

  def main(args: Array[String]): Unit = {
    val port = 8080

    println("Starting Apache Spark session for FlightWebApp...")
    val spark = SparkSession.builder()
      .appName("FlightWebApp")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val server = HttpServer.create(new InetSocketAddress(port), 0)

    server.createContext("/", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        val method = exchange.getRequestMethod.toUpperCase
        if (method == "GET") {
          val html = renderPage(DefaultSample)
          val bytes = html.getBytes(StandardCharsets.UTF_8)
          exchange.getResponseHeaders.set("Content-Type", "text/html; charset=UTF-8")
          exchange.sendResponseHeaders(200, bytes.length)
          val os = exchange.getResponseBody
          os.write(bytes)
          os.close()
        } else {
          exchange.sendResponseHeaders(405, -1)
        }
      }
    })

    server.createContext("/analyze", new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        val method = exchange.getRequestMethod.toUpperCase
        if (method == "POST") {
          val body = Source.fromInputStream(exchange.getRequestBody, StandardCharsets.UTF_8.name()).mkString
          val form = parseFormData(body)
          val rawInput = form.getOrElse("routes", "")

          try {
            val (reportText, summary, warnings) = FlightNetworkAnalysis.analyzeText(sc, rawInput)
            val html = renderPage(rawInput, Some((summary, reportText, warnings)))
            val bytes = html.getBytes(StandardCharsets.UTF_8)
            exchange.getResponseHeaders.set("Content-Type", "text/html; charset=UTF-8")
            exchange.sendResponseHeaders(200, bytes.length)
            val os = exchange.getResponseBody
            os.write(bytes)
            os.close()
          } catch {
            case NonFatal(e) =>
              val html = renderPage(rawInput, None, Some(e.getMessage))
              val bytes = html.getBytes(StandardCharsets.UTF_8)
              exchange.getResponseHeaders.set("Content-Type", "text/html; charset=UTF-8")
              exchange.sendResponseHeaders(200, bytes.length)
              val os = exchange.getResponseBody
              os.write(bytes)
              os.close()
          }
        } else {
          exchange.sendResponseHeaders(405, -1)
        }
      }
    })

    server.setExecutor(null)
    server.start()

    println(s"\nOpen http://localhost:$port\n")
    println("FlightNetworkAnalysis interactive server is running.")
    println("Press Ctrl+C or kill the process to stop the server.")

    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      println("\nShutting down server and SparkContext...")
      server.stop(0)
      spark.stop()
    }))
  }
}
