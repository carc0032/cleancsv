# CleanCSV.py
from __future__ import annotations

import csv
import hashlib
import json
import logging
import os
import re
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, List

import pandas as pd
import stripe
from flask import Flask, abort, redirect, render_template_string, request, send_file

app = Flask(__name__)

# ============================
# Logging (JSON lines)
# ============================
LOG_LEVEL = os.environ.get("CLEANCCSV_LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), format="%(message)s")
logger = logging.getLogger("cleancsv")


def _safe_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_client_ip() -> str:
    xff = request.headers.get("X-Forwarded-For", "")
    if xff:
        return xff.split(",")[0].strip()
    return request.remote_addr or "unknown"


def ip_hash(ip: str) -> str:
    return hashlib.sha256(ip.encode("utf-8")).hexdigest()[:12]


def log_event(event: str, **fields: Any) -> None:
    payload = {"ts": _safe_now(), "event": event, "service": "cleancsv"}
    try:
        payload["path"] = request.path
        payload["method"] = request.method
        payload["ua"] = request.headers.get("User-Agent", "")
        payload["ip_hash"] = ip_hash(get_client_ip())
    except Exception:
        pass

    payload.update(fields)
    logger.info(json.dumps(payload, default=str))


# ============================
# Config
# ============================
WORK_DIR = Path(os.environ.get("CLEANCCSV_WORK_DIR", "/tmp")) / "cleancsv"
WORK_DIR.mkdir(parents=True, exist_ok=True)

MAX_BYTES = int(os.environ.get("CLEANCCSV_MAX_BYTES", str(20 * 1024 * 1024)))
RETENTION_MINUTES = int(os.environ.get("CLEANCCSV_RETENTION_MINUTES", "30"))

MAX_ROWS = int(os.environ.get("CLEANCCSV_MAX_ROWS", "200000"))
MAX_COLS = int(os.environ.get("CLEANCCSV_MAX_COLS", "300"))

RATE_WINDOW_SECONDS = int(os.environ.get("CLEANCCSV_RATE_WINDOW_SECONDS", "60"))
RATE_MAX_UPLOADS = int(os.environ.get("CLEANCCSV_RATE_MAX_UPLOADS", "10"))

stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")
PRICE_ID = os.environ.get("STRIPE_PRICE_ID", "")
BASE_URL = os.environ.get("APP_BASE_URL", "http://127.0.0.1:5000")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
PAYMENTS_ENABLED = bool(stripe.api_key and PRICE_ID)

SUPPORT_EMAIL = "carney.christopher22@gmail.com"

_upload_hits = defaultdict(deque)

# ============================
# HTML
# ============================
INDEX_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>CleanCSV — Repair malformed CSV files</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />

  <style>
    body {
      font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
      margin: 0;
      background: #fff;
      color: #111;
    }

    /* Top utility header */
    .topbar {
      border-bottom: 1px solid #e5e7eb;
      padding: 12px 20px;
      font-size: 14px;
      background: #fff;
    }
    .topbar a {
      color: #666;
      text-decoration: none;
    }

    .wrap {
      max-width: 1200px;
      margin: 0 auto;
      padding: 20px 24px;
    }

    .hero {
      display: grid;
      grid-template-columns: 1fr 520px;
      gap: 32px;
      align-items: start;
    }

    @media (max-width: 900px) {
      .hero {
        grid-template-columns: 1fr;
      }
    }

    h1 {
      font-size: 34px;
      line-height: 1.15;
      margin: 0 0 8px;
      letter-spacing: -0.01em;
    }

    .sub {
      color: #555;
      font-size: 16px;
      margin: 0 0 14px;
      line-height: 1.5;
    }

    .bullets {
      padding-left: 18px;
      margin: 0;
    }
    .bullets li {
      margin: 8px 0;
      font-size: 14px;
      color: #444;
    }

    .card {
      border: 1px solid #d1d5db;
      border-radius: 6px;
      padding: 16px;
      background: #f9fafb;
    }

    .label {
      font-size: 13px;
      color: #666;
      margin: 0 0 8px;
    }

    input[type=file] {
      width: 100%;
    }

    .row {
      margin-top: 14px;
    }

    .opt {
      margin-top: 12px;
    }

    .opt .help {
      font-size: 13px;
      color: #666;
      margin: 6px 0 0 22px;
      line-height: 1.4;
    }

    .btn {
      display: inline-block;
      padding: 10px 14px;
      border-radius: 4px;
      border: 1px solid #374151;
      background: #374151;
      color: #fff;
      cursor: pointer;
      font-weight: 500;
    }

    .muted {
      color: #666;
      font-size: 14px;
      line-height: 1.45;
    }

    .error {
      color: #b00020;
      white-space: pre-wrap;
      margin: 12px 0 0;
    }

    footer {
      border-top: 1px solid #f0f0f0;
      margin-top: 28px;
      padding-top: 18px;
      color: #666;
      font-size: 13px;
      display: flex;
      flex-wrap: wrap;
      gap: 10px 18px;
      justify-content: space-between;
    }

    .footer-left {
      display: flex;
      flex-wrap: wrap;
      gap: 10px 18px;
    }

    .pill {
      display: inline-block;
      padding: 4px 10px;
      border: 1px solid #e5e7eb;
      border-radius: 999px;
      font-size: 12px;
      color: #444;
      background: #fafafa;
    }
  </style>
</head>

<body>

  <!-- Top utility header -->
  <div class="topbar">
    <strong>CleanCSV</strong>
    <span style="color:#666; margin-left:12px;">CSV repair utility</span>
    <span style="float:right;">
      <a href="/problems">Problems</a>
    </span>
  </div>

  <div class="wrap">
    <div class="hero">

      <!-- Left: context -->
      <div>
        <h1>Repair malformed CSV files</h1>

        <p class="sub">
          Fix delimiter, encoding, and row-structure issues so CSV files import correctly.
          <br />
          <strong>Handles European CSVs automatically</strong> (semicolon delimiters, decimal commas, localized number formats).
        </p>

        <ul class="bullets">
          <li>Strict row normalization (every row matches the header)</li>
          <li>Explicit delimiter detection (comma, semicolon, tab, pipe)</li>
          <li>RFC-style handling of quoted multiline fields</li>
        </ul>
      </div>

      <!-- Right: tool -->
      <div class="card">
        <p class="label"><strong>Process CSV</strong></p>

        <form action="/upload" method="post" enctype="multipart/form-data">
          <input type="file" name="file" required
                 accept=".csv,.tsv,.txt,text/csv,text/tab-separated-values,text/plain" />

          <div class="opt">
            <label>
              <input type="checkbox" name="near_dupes_preview" value="1" />
              Preview near-duplicates (dry run)
            </label>
            <div class="help">Shows what would be removed without modifying the file.</div>
          </div>

          <div class="opt">
            <label>
              <input type="checkbox" name="near_dupes_remove" value="1" />
              Remove near-duplicates
            </label>
            <div class="help">Removes rows identical after ignoring ID/date/balance fields.</div>
          </div>

          <div class="opt">
            <label>
              <input type="checkbox" name="normalize_numbers" value="1" />
              Normalize numbers
            </label>
            <div class="help">Converts values like 1.234,56 → 1234.56 where safe.</div>
          </div>

          <div class="row">
            <button class="btn" type="submit">Process file</button>
          </div>
        </form>

        <p class="muted" style="margin-top:8px;">
          Output: UTF-8 CSV with normalized structure
        </p>

        {% if error %}
          <p class="error"><strong>Error:</strong> {{ error }}</p>
        {% endif %}

        <div class="row muted">
          <span class="pill">Max {{ max_mb }} MB</span>
          <span class="pill">{{ max_rows }} rows / {{ max_cols }} cols</span>
        </div>

        <p class="muted" style="margin-top:12px;">
          {% if payments_enabled %}
            Download requires a one-time fee.
          {% else %}
            Payments disabled. Downloads are free.
          {% endif %}
        </p>
      </div>

    </div>

    <footer>
      <div class="footer-left">
        <span>Files are not stored.</span>
        <span>Support: {{ support_email }}</span>
        <span>CSV/TSV only</span>
        <span><a href="/problems">Common CSV import problems</a></span>
      </div>
      <div>
        <span class="pill">Secure checkout via Stripe</span>
      </div>
    </footer>

  </div>
</body>
</html>
"""
PROBLEM_EXPECTED_FIELDS_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Fix CSV import error: “Expected 3 fields, saw 5”</title>
  <meta name="description" content="Explanation of the CSV import error “Expected 3 fields, saw 5” and how to fix it by repairing inconsistent rows and malformed records." />
  <link rel="canonical" href="{{ BASE_URL }}/problems/expected-fields-error" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 0; background:#fff; color:#111; }
    .wrap { max-width: 760px; margin: 0 auto; padding: 48px 20px; }
    h1 { font-size: 32px; margin-bottom: 16px; }
    p { line-height: 1.6; margin: 12px 0; }
    ul { margin: 12px 0 16px 20px; }
    pre {
      background: #f8f8f8;
      border: 1px solid #e5e7eb;
      border-radius: 8px;
      padding: 12px;
      font-size: 13px;
      overflow-x: auto;
    }
    .note {
      background: #fafafa;
      border-left: 4px solid #ddd;
      padding: 12px;
      margin: 20px 0;
    }
    .quiet-link {
      margin-top: 28px;
      font-size: 14px;
    }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Fix CSV import error: “Expected 3 fields, saw 5”</h1>

    <p>
      This error occurs when rows in a CSV file contain more columns than the header row.
      Many tools stop importing when this happens.
    </p>

    <p>Common causes include:</p>
    <ul>
      <li>Extra delimiters inside text fields</li>
      <li>Line breaks inside quoted values</li>
      <li>Inconsistent exports from reporting tools</li>
    </ul>

    <p><b>Example of a broken CSV</b></p>

    <pre>
id,name,amount
1,"ACME, Inc",100
2,Widget,200,EXTRA
    </pre>

    <p><b>After repairing the file</b></p>

    <pre>
id,name,amount
1,ACME, Inc,100
2,Widget,200
    </pre>

    <div class="note">
      The fix is to repair row structure so every row matches the header
      and malformed records are stitched back together correctly.
    </div>

    <div class="quiet-link">
      If you want to fix the file automatically, you can upload it here:
      <a href="/">CleanCSV</a>
    </div>
  </div>
</body>
</html>
"""

PROBLEM_EXCEL_ONE_COLUMN_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Excel CSV opens in one column — wrong delimiter</title>
  <meta name="description" content="Why Excel opens some CSV files in a single column and how to fix delimiter issues so data imports into separate columns correctly." />
  <link rel="canonical" href="{{ BASE_URL }}/problems/excel-one-column-csv" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 0; background:#fff; color:#111; }
    .wrap { max-width: 760px; margin: 0 auto; padding: 48px 20px; }
    h1 { font-size: 32px; margin-bottom: 16px; }
    p { line-height: 1.6; margin: 12px 0; }
    ul { margin: 12px 0 16px 20px; }
    pre { background: #f8f8f8; border: 1px solid #e5e7eb; border-radius: 8px; padding: 12px; font-size: 13px; overflow-x: auto; }
    .note { background: #fafafa; border-left: 4px solid #ddd; padding: 12px; margin: 20px 0; }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Excel CSV opens in one column — wrong delimiter</h1>

    <p>
      If your CSV opens entirely in a single column in Excel, it usually means
      Excel guessed the wrong delimiter.
    </p>

    <p>This often happens when:</p>
    <ul>
      <li>The file uses semicolons instead of commas</li>
      <li>The system locale expects decimal commas</li>
      <li>The CSV was exported from European software</li>
    </ul>

    <p><b>Example CSV</b></p>
    <pre>
id;amount;description
1;1.234,56;Invoice
2;12,34;Refund
    </pre>

    <p><b>After fixing the delimiter</b></p>
    <pre>
id,amount,description
1,1234.56,Invoice
2,12.34,Refund
    </pre>

    <div class="note">
      The fix is to detect the correct delimiter and normalize numeric formats
      so Excel and other tools parse the file correctly.
    </div>

    <p>
      To fix the file automatically, upload it here:
      <a href="/">CleanCSV</a>
    </p>
  </div>
</body>
</html>
"""
PROBLEM_EXPECTED_FIELDS_SAW_FIELDS_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Fix CSV error: Expected N fields in line X, saw Y</title>
  <meta name="description" content="Why the CSV error “Expected N fields in line X, saw Y” occurs in pandas and other tools, and how to repair the file so it imports correctly." />
  <link rel="canonical" href="{{ BASE_URL }}/problems/expected-fields-saw-fields" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    body {
      font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
      margin: 0;
      background: #fff;
      color: #111;
    }
    .wrap {
      max-width: 760px;
      margin: 0 auto;
      padding: 48px 20px;
    }
    h1 {
      font-size: 32px;
      margin-bottom: 16px;
    }
    p {
      line-height: 1.6;
      margin: 12px 0;
    }
    ul {
      margin: 12px 0 16px 20px;
    }
    pre {
      background: #f8f8f8;
      border: 1px solid #e5e7eb;
      border-radius: 8px;
      padding: 12px;
      font-size: 13px;
      overflow-x: auto;
    }
    .note {
      background: #fafafa;
      border-left: 4px solid #ddd;
      padding: 12px;
      margin: 20px 0;
    }
    .muted {
      color: #666;
      font-size: 14px;
    }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Fix CSV error: “Expected N fields in line X, saw Y”</h1>

    <p>
      This error means that not all rows in your CSV file have the same number of columns.
      Strict parsers (like pandas, Excel, Power BI, and databases) stop importing when this happens.
    </p>

    <p>Common causes include:</p>
    <ul>
      <li>Extra delimiters inside text fields</li>
      <li>Newlines inside quoted values</li>
      <li>Wrong delimiter guessed (comma vs semicolon vs tab)</li>
      <li>Report-style exports with header text above the real CSV header</li>
    </ul>

    <p class="muted">You may see an error like:</p>
    <pre>Error tokenizing data. C error: Expected 13 fields in line 64, saw 15</pre>

    <p><b>Example of a CSV that triggers this error</b></p>
    <pre>
id,name,amount
1,"ACME, Inc",100
2,Widget,200,EXTRA
    </pre>

    <p><b>After repairing the file</b></p>
    <pre>
id,name,amount
1,ACME, Inc,100
2,Widget,200
    </pre>

    <div class="note">
      The fix is to normalize row structure so every row matches the header,
      stitch multiline quoted records back into a single row,
      and ensure the correct delimiter is used.
    </div>

    <p>
      To fix the file automatically, upload it here:
      <a href="/">CleanCSV</a>
    </p>
  </div>
</body>
</html>
"""

PROBLEM_EXPECTED_FIELDS_SAW_FIELDS_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Fix CSV error: Expected N fields in line X, saw Y (pandas / import)</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 0; background:#fff; color:#111; }
    .wrap { max-width: 760px; margin: 0 auto; padding: 48px 20px; }
    h1 { font-size: 32px; margin-bottom: 16px; }
    p { line-height: 1.6; margin: 12px 0; }
    ul { margin: 12px 0 16px 20px; }
    pre { background: #f8f8f8; border: 1px solid #e5e7eb; border-radius: 8px; padding: 12px; font-size: 13px; overflow-x: auto; }
    code { background:#f1f5f9; padding: 2px 6px; border-radius: 6px; }
    .note { background: #fafafa; border-left: 4px solid #ddd; padding: 12px; margin: 20px 0; }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Fix CSV error: “Expected N fields in line X, saw Y”</h1>

    <p>
      This error means your CSV rows don’t all have the same number of columns.
      It commonly appears when importing with pandas (<code>read_csv</code>), Excel, Power BI,
      databases, or other strict parsers.
    </p>

    <p>Typical causes:</p>
    <ul>
      <li>Extra delimiters in some rows (e.g., commas inside unquoted text)</li>
      <li>Newlines inside quoted fields (a single record spans multiple lines)</li>
      <li>Wrong delimiter guessed (comma vs semicolon vs tab)</li>
      <li>Report-style exports with preamble lines above the real header</li>
    </ul>
    <p><b>Common pandas error message</b></p>
    <pre>Error tokenizing data. C error: Expected 13 fields in line 64, saw 15</pre>
    
    <p><b>Example of a file that triggers the error</b></p>
    <pre>
id,name,amount
1,"ACME, Inc",100
2,Widget,200,EXTRA
    </pre>

    <p><b>After repair</b></p>
    <pre>
id,name,amount
1,ACME, Inc,100
2,Widget,200
    </pre>

    <div class="note">
      The fix is to normalize row structure (pad/truncate to the header width),
      stitch multiline quoted records back into single rows, and ensure the correct delimiter is used.
    </div>

    <p>
      To fix the file automatically, upload it here:
      <a href="/">CleanCSV</a>
    </p>
  </div>
</body>
</html>
"""
PROBLEM_CSV_ENCODING_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Fix CSV encoding issues (�, UTF-8 vs Windows-1252)</title>
  <meta name="description" content="Why CSV files show weird characters like � and how to fix encoding issues by converting Windows-1252 and other encodings to UTF-8." />
  <link rel="canonical" href="{{ BASE_URL }}/problems/csv-encoding-utf8-windows-1252" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 0; background:#fff; color:#111; }
    .wrap { max-width: 760px; margin: 0 auto; padding: 48px 20px; }
    h1 { font-size: 32px; margin-bottom: 16px; }
    p { line-height: 1.6; margin: 12px 0; }
    ul { margin: 12px 0 16px 20px; }
    pre { background: #f8f8f8; border: 1px solid #e5e7eb; border-radius: 8px; padding: 12px; font-size: 13px; overflow-x: auto; }
    code { background:#f1f5f9; padding: 2px 6px; border-radius: 6px; }
    .note { background: #fafafa; border-left: 4px solid #ddd; padding: 12px; margin: 20px 0; }
    .muted { color: #666; font-size: 14px; }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Fix CSV encoding issues (�, UTF-8 vs Windows-1252)</h1>

    <p>
      If your CSV imports with weird characters (like <code>�</code>), broken quotes, or garbled symbols,
      the file is likely using a different text encoding than your tool expects.
    </p>

    <p>Common symptoms:</p>
    <ul>
      <li>Curly quotes turn into <code>�</code> or random symbols</li>
      <li>Currency symbols like <code>€</code> or <code>£</code> don’t display correctly</li>
      <li>Accented characters (e.g., <code>café</code>) appear corrupted</li>
    </ul>

    <p class="muted">You might see characters like this:</p>
    <pre>Alice,â€œsmart quotesâ€ â€” cafÃ©</pre>

    <p><b>After decoding properly</b></p>
    <pre>Alice,“smart quotes” — café</pre>

    <div class="note">
      The fix is to detect the correct encoding (often <b>Windows-1252</b> / <b>cp1252</b> for exports from older systems),
      decode the file safely, and re-save as UTF-8 for maximum compatibility.
    </div>

    <p>
      To fix the file automatically, upload it here:
      <a href="/">CleanCSV</a>
    </p>
  </div>
</body>
</html>
"""

PROBLEM_POWERBI_DECIMAL_COMMA_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Power BI CSV decimal comma issue — fix import parsing</title>
  <meta name="description" content="Fix Power BI CSV import problems caused by decimal commas and European number formats by normalizing delimiters and numeric values." />
  <link rel="canonical" href="{{ BASE_URL }}/problems/powerbi-decimal-comma-csv" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 0; background:#fff; color:#111; }
    .wrap { max-width: 760px; margin: 0 auto; padding: 48px 20px; }
    h1 { font-size: 32px; margin-bottom: 16px; }
    p { line-height: 1.6; margin: 12px 0; }
    ul { margin: 12px 0 16px 20px; }
    pre { background: #f8f8f8; border: 1px solid #e5e7eb; border-radius: 8px; padding: 12px; font-size: 13px; overflow-x: auto; }
    .note { background: #fafafa; border-left: 4px solid #ddd; padding: 12px; margin: 20px 0; }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Power BI CSV decimal comma issue (EU number formats)</h1>

    <p>
      If Power BI imports your CSV but numbers come in as text, split incorrectly, or show errors,
      the file may be using <b>European number formatting</b>:
      decimal commas (e.g., <code>12,34</code>) and often semicolon delimiters (e.g., <code>;</code>).
    </p>

    <p>Common symptoms:</p>
    <ul>
      <li>Numeric columns import as text</li>
      <li>Values like <code>1.234,56</code> don’t parse as numbers</li>
      <li>Columns shift because Power BI guessed the wrong delimiter</li>
    </ul>

    <p><b>Example CSV (EU format)</b></p>
    <pre>
id;amount;description
1;1.234,56;Invoice
2;12,34;Refund
3;(5,00);Chargeback
    </pre>

    <p><b>After normalization (Power BI-friendly)</b></p>
    <pre>
id,amount,description
1,1234.56,Invoice
2,12.34,Refund
3,-5.00,Chargeback
    </pre>

    <div class="note">
      The fix is to detect the correct delimiter and normalize numeric formats
      (decimal commas, thousands separators, parentheses negatives) so Power BI can type the column as numeric.
    </div>

    <p>
      To fix the file automatically, upload it here:
      <a href="/">CleanCSV</a>
    </p>
  </div>
</body>
</html>
"""
PROBLEMS_INDEX_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>CSV import problems CleanCSV fixes</title>
  <meta name="description" content="A list of common CSV import errors and explanations of how to fix delimiter, encoding, and malformed record issues." />
  <link rel="canonical" href="{{ BASE_URL }}/problems" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    body {
      font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
      margin: 0;
      background: #fff;
      color: #111;
    }
    .wrap {
      max-width: 760px;
      margin: 0 auto;
      padding: 48px 20px;
    }
    h1 {
      font-size: 32px;
      margin-bottom: 12px;
    }
    p {
      line-height: 1.6;
      margin: 12px 0;
    }
    ul {
      margin: 20px 0;
      padding-left: 18px;
    }
    li {
      margin: 12px 0;
    }
    a {
      color: #111;
      text-decoration: underline;
    }
    .muted {
      color: #666;
      font-size: 14px;
    }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>CSV import problems</h1>

    <p>
      These pages explain common CSV import errors and how to fix them.
      Each page describes the issue and provides a way to repair the file automatically.
    </p>

    <ul>
      <li>
        <a href="/problems/expected-fields-error">
          CSV import error: “Expected 3 fields, saw 5”
        </a>
      </li>
      <li>
        <a href="/problems/expected-fields-saw-fields">
          pandas error: “Expected N fields in line X, saw Y”
        </a>
      </li>
      <li>
        <a href="/problems/excel-one-column-csv">
          Excel CSV opens in one column (wrong delimiter)
        </a>
      </li>
      <li>
        <a href="/problems/powerbi-decimal-comma-csv">
          Power BI CSV decimal comma issue (EU number formats)
        </a>
      </li>
      <li>
        <a href="/problems/csv-encoding-utf8-windows-1252">
          CSV encoding issues (�, UTF-8 vs Windows-1252)
        </a>
      </li>
    </ul>

    <p class="muted">
      To fix a file, upload it here:
      <a href="/">CleanCSV</a>
    </p>
  </div>
</body>
</html>
"""
RESULT_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>CleanCSV — Results</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />

  <style>
    body {
      font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
      margin: 0;
      background: #fff;
      color: #111;
    }

    .wrap {
      max-width: 1120px;
      margin: 0 auto;
      padding: 32px 20px 28px;
    }

    .card {
      border: 1px solid #e5e7eb;
      border-radius: 14px;
      padding: 18px;
      background: #fff;
      margin-top: 16px;
    }

    .btn {
      display: inline-block;
      padding: 10px 14px;
      border-radius: 4px;
      border: 1px solid #374151;
      background: #374151;
      color: #fff;
      text-decoration: none;
      font-weight: 500;
    }

    .muted {
      color: #666;
      font-size: 14px;
      line-height: 1.45;
    }

    .pill {
      display: inline-block;
      padding: 4px 10px;
      border: 1px solid #e5e7eb;
      border-radius: 999px;
      font-size: 12px;
      color: #444;
      background: #fafafa;
      margin-right: 8px;
    }

    .warn {
      color: #8a6d3b;
    }

    .subhead {
      font-weight: 700;
      margin: 14px 0 6px;
    }

    .table-wrap {
      overflow-x: auto;
      width: 100%;
      border: 1px solid #e5e7eb;
      border-radius: 10px;
      padding: 8px;
      background: #fff;
    }

    .table-wrap table {
      border-collapse: collapse;
      width: max-content;
      min-width: 100%;
    }

    .table-wrap th,
    .table-wrap td {
      border-bottom: 1px solid #eee;
      padding: 8px;
      text-align: left;
      font-size: 13px;
      white-space: nowrap;
    }

    /* Change log styling */
    .change-log {
      margin: 6px 0 0 18px;
    }

    .change-log li {
      margin: 6px 0;
    }

    .change-fix {
      color: #047857;
      font-weight: 500;
    }

    .change-ok {
      color: #374151;
    }

    footer {
      border-top: 1px solid #f0f0f0;
      margin-top: 28px;
      padding-top: 18px;
      color: #666;
      font-size: 13px;
      display: flex;
      flex-wrap: wrap;
      gap: 10px 18px;
      justify-content: space-between;
    }

    .footer-left {
      display: flex;
      flex-wrap: wrap;
      gap: 10px 18px;
    }
  </style>
</head>

<body>
  <div class="wrap">
    <h1 style="margin:0 0 6px;">Results</h1>

    <p class="muted" style="margin:0 0 10px;">
      <span class="pill">Job: {{ job_id }}</span>
      <span class="pill">Paid: {{ "yes" if paid else "no" }}</span>
      {% if import_warning %}<span class="pill warn">Import repaired</span>{% endif %}
      {% if near_dupes_mode %}<span class="pill">Near-dupes: {{ near_dupes_mode }}</span>{% endif %}
      {% if detected_delimiter %}<span class="pill">Delimiter: {{ detected_delimiter_label }}</span>{% endif %}
    </p>

    <p class="muted" style="margin:0 0 12px;">
      <span class="pill">Encoding: {{ encoding_label }}</span>
      <span class="pill">Numbers: {{ numbers_label }}</span>
      <span class="pill">Header: {{ header_label }}</span>
    </p>

    <div class="card">
      <p style="margin:0;">
        <strong>Rows:</strong> {{ rows }} &nbsp;
        <strong>Columns:</strong> {{ cols }}
      </p>

      <p class="muted" style="margin-top:10px;">
        Summary of validation and repair steps:
      </p>

      <!-- Repairs applied -->
      <p class="subhead">Repairs applied</p>
      <ul class="change-log">
        {% for item in changelog %}
          {% if "Fixed" in item
             or "Removed" in item
             or "Stitched" in item
             or "Normalized" in item
             or "Header detected" in item
             or "Quote stitching" in item
             or "Converted to UTF-8" in item %}
            <li class="change-fix">{{ item }}</li>
          {% endif %}
        {% endfor %}
      </ul>

      <!-- Checks passed -->
      <p class="subhead">Checks passed</p>
      <ul class="change-log">
        {% for item in changelog %}
          {% if not (
             "Fixed" in item
             or "Removed" in item
             or "Stitched" in item
             or "Normalized" in item
             or "Header detected" in item
             or "Quote stitching" in item
             or "Converted to UTF-8" in item ) %}
            <li class="change-ok">{{ item }}</li>
          {% endif %}
        {% endfor %}
      </ul>

      <div style="margin-top:16px;">
        <a class="btn" href="/download/{{ job_id }}">
          {% if paid or not payments_enabled %}
            Download cleaned file
          {% else %}
            Pay $5 & download
          {% endif %}
        </a>

        <p class="muted" style="margin-top:8px;">
          This file reflects the changes listed above.
        </p>
      </div>

      <p class="muted" style="margin-top:12px;">
        {% if payments_enabled %}
          $5 one-time download
        {% else %}
          Payments disabled. Downloads are free.
        {% endif %}
      </p>
    </div>

    {% if near_dupe_examples %}
      <div class="card">
        <p class="subhead">Near-duplicate examples</p>
        <p class="muted" style="margin-top:0;">
          Each example shows two rows. “Kept” is the first occurrence;
          the other is {{ "what would be removed" if near_dupes_mode == "preview" else "what was removed" }}.
        </p>

        {% for ex in near_dupe_examples %}
          <div style="margin-top:14px;">
            <div class="table-wrap">{{ ex|safe }}</div>
          </div>
        {% endfor %}
      </div>
    {% endif %}

    <div class="card">
      <p class="subhead">Preview: first 10 rows</p>
      <div class="table-wrap">{{ preview_first|safe }}</div>
    </div>

    <div class="card">
      <p class="subhead">Preview: last 10 rows</p>
      <div class="table-wrap">{{ preview_last|safe }}</div>
    </div>

    {% if preview_repaired %}
      <div class="card">
        <p class="subhead">Preview: repaired rows (up to 10)</p>
        <p class="muted" style="margin-top:0;">
          These rows had the wrong number of columns and were repaired.
        </p>
        <div class="table-wrap">{{ preview_repaired|safe }}</div>
      </div>
    {% endif %}

    <footer>
      <div class="footer-left">
        <span>Files are not stored.</span>
        <span>Support: {{ support_email }}</span>
      </div>
      <div>
        <span class="pill">Secure checkout via Stripe</span>
      </div>
    </footer>
  </div>
</body>
</html>
"""

SUCCESS_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>CleanCSV — Payment received</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 0; background:#fff; color:#111; }
    .wrap { max-width: 920px; margin: 0 auto; padding: 40px 20px 28px; }
    .btn { display:inline-block; padding: 10px 14px; border-radius: 10px; border: 1px solid #111; background: #111; color: #fff; text-decoration:none; font-weight:600; }
    .muted { color: #666; font-size: 14px; line-height:1.45; }
    footer { border-top: 1px solid #f0f0f0; margin-top: 28px; padding-top: 18px; color: #666; font-size: 13px; }
  </style>
</head>
<body>
  <div class="wrap">
    <h1 style="margin:0 0 8px;">Payment received</h1>
    <p class="muted" style="margin-top:0;">You're good to go.</p>
    <p><a class="btn" href="/download/{{ job_id }}">Download cleaned file</a></p>
    <p class="muted">Need help? Email {{ support_email }} and include Job ID: {{ job_id }}</p>
    <p class="muted"><a href="/result/{{ job_id }}">Back to results</a></p>
    <footer>Files are not stored • Support: {{ support_email }}</footer>
  </div>
</body>
</html>
"""

CANCEL_HTML = """
<!doctype html>
<html>
<head><meta charset="utf-8" /><title>CleanCSV — Payment canceled</title></head>
<body style="font-family:system-ui,sans-serif;margin:40px;">
  <h1>Payment canceled</h1>
  <p><a href="/">Back to upload</a></p>
</body>
</html>
"""

# ============================
# Rate limiting + file checks
# ============================
def rate_limit_check(ip: str) -> bool:
    now = time.time()
    q = _upload_hits[ip]
    while q and (now - q[0]) > RATE_WINDOW_SECONDS:
        q.popleft()
    if len(q) >= RATE_MAX_UPLOADS:
        return False
    q.append(now)
    return True


def looks_like_text_file(path: Path, sample_bytes: int = 4096) -> bool:
    try:
        b = path.read_bytes()[:sample_bytes]
    except Exception:
        return False
    return b"\x00" not in b


def looks_like_csv_name(filename: str) -> bool:
    fn = (filename or "").lower().strip()
    return fn.endswith(".csv") or fn.endswith(".tsv") or fn.endswith(".txt")


# ============================
# Storage helpers
# ============================
def cleanup_old_files() -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=RETENTION_MINUTES)
    for p in WORK_DIR.glob("*"):
        try:
            mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
            if mtime < cutoff:
                p.unlink(missing_ok=True)
        except Exception:
            pass


def bytes_too_large(req) -> bool:
    cl = req.content_length
    return (cl is not None) and (cl > MAX_BYTES)


def out_path(job_id: str) -> Path:
    return WORK_DIR / f"{job_id}.clean.csv"


def raw_path(job_id: str) -> Path:
    return WORK_DIR / f"{job_id}.raw"


def norm_path(job_id: str) -> Path:
    return WORK_DIR / f"{job_id}.normalized"


def manifest_path(job_id: str) -> Path:
    return WORK_DIR / f"{job_id}.json"


def _json_safe(v: Any) -> Any:
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    return v


def write_manifest(job_id: str, data: dict[str, Any]) -> None:
    base = {
        "job_id": job_id,
        "created_at": _safe_now(),
        "paid": False,
        "paid_at": None,
        "stripe_session_id": None,
        "stripe_event_id": None,
        "rows": None,
        "cols": None,
        "changelog": [],
        "import_warning": False,
        "repaired_row_indices": [],
        "near_dupes_mode": "",
        "ignored_cols": [],
        "near_dupes_count": 0,
        "near_dupe_examples_rows": [],
        "detected_delimiter": ",",
        "out_file": out_path(job_id).name,
        "original_file": raw_path(job_id).name,
        "original_filename": "original.csv",
    }
    base.update(data)
    manifest_path(job_id).write_text(json.dumps(base, indent=2), encoding="utf-8")


def read_manifest(job_id: str) -> dict[str, Any]:
    p = manifest_path(job_id)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def mark_paid(job_id: str, session_id: str | None = None, event_id: str | None = None) -> None:
    m = read_manifest(job_id)
    if not m or m.get("paid"):
        return
    m["paid"] = True
    m["paid_at"] = _safe_now()
    if session_id:
        m["stripe_session_id"] = session_id
    if event_id:
        m["stripe_event_id"] = event_id
    manifest_path(job_id).write_text(json.dumps(m, indent=2), encoding="utf-8")


# ============================
# Quote stitching + encoding normalization
# ============================
def stitch_csv_records(text: str) -> tuple[str, dict]:
    lines = text.split("\n")
    physical = len(lines)

    logical_lines: list[str] = []
    buf_parts: list[str] = []
    in_quotes = False
    current_physical_count = 0
    stitched_records = 0
    max_physical = 1

    def process_line_for_quotes(line: str, in_q: bool) -> bool:
        i = 0
        while i < len(line):
            if line[i] == '"':
                # Escaped quote: ""
                if i + 1 < len(line) and line[i + 1] == '"':
                    i += 2
                    continue
                in_q = not in_q
            i += 1
        return in_q

    for line in lines:
        current_physical_count += 1

        if not buf_parts:
            buf_parts.append(line)
        else:
            buf_parts.append("\n" + line)

        in_quotes = process_line_for_quotes(line, in_quotes)

        if not in_quotes:
            combined = "".join(buf_parts)
            logical_lines.append(combined)

            if current_physical_count > 1:
                stitched_records += 1
                max_physical = max(max_physical, current_physical_count)

            buf_parts = []
            current_physical_count = 0

    if buf_parts:
        logical_lines.append("".join(buf_parts))
        if current_physical_count > 1:
            stitched_records += 1
            max_physical = max(max_physical, current_physical_count)

    stitched_text = "\n".join(logical_lines)
    stats = {
        "physical_lines": physical,
        "logical_lines": len(logical_lines),
        "stitched_records": stitched_records,
        "max_physical_per_record": max_physical,
    }
    return stitched_text, stats


def decode_text_with_fallback(path: Path) -> tuple[str, str]:
    data = path.read_bytes()
    candidates = ["utf-8-sig", "utf-8", "cp1252", "latin-1"]
    last_err: Exception | None = None

    for enc in candidates:
        try:
            return data.decode(enc, errors="strict"), enc
        except Exception as e:
            last_err = e

    raise ValueError(f"Could not decode file using common encodings: {last_err}")


def normalize_to_utf8_lf(src: Path, dst: Path) -> tuple[Path, list[str], str, dict]:
    log: list[str] = []
    text, enc = decode_text_with_fallback(src)

    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    if normalized != text:
        log.append("Normalized line endings (fixed Windows/Mac-style newlines).")

    if enc != "utf-8":
        log.append(f"Detected encoding: {enc}. Converted to UTF-8 for output compatibility.")
    else:
        log.append("Detected encoding: utf-8.")

    stitched, stitch_stats = stitch_csv_records(normalized)
    if stitch_stats["stitched_records"] > 0:
        log.append(
            f'Quote stitching: merged {stitch_stats["physical_lines"]} physical lines into '
            f'{stitch_stats["logical_lines"]} logical rows '
            f'(records stitched: {stitch_stats["stitched_records"]}, '
            f'max lines/record: {stitch_stats["max_physical_per_record"]}).'
        )
    else:
        log.append("Quote stitching: no multi-line quoted records detected.")

    dst.write_text(stitched, encoding="utf-8", newline="\n")
    return dst, log, enc, stitch_stats


# ============================
# Delimiter detection + lenient parsing
# ============================
_CANDIDATE_DELIMS = [",", ";", "\t", "|"]

def detect_and_strip_preamble(
    text: str,
    delimiter: str,
    max_scan_lines: int = 60
) -> tuple[str, list[str], dict]:
    """
    Quote-aware header detection:
    - Uses csv.reader to compute field counts (respects quotes)
    - Finds modal field count among first N lines
    - Scores candidate header rows
    - Strips preamble only when confident
    Returns (cleaned_text, log_lines, header_info)
    """
    log: list[str] = []
    lines = text.splitlines()

    if not lines:
        log.append("Header detection: empty file.")
        return text, log, {}

    scan_lines = lines[:max_scan_lines]

    # Quote-aware field count per line
    field_counts: list[tuple[int, int]] = []  # (line_index, field_count)
    for i, ln in enumerate(scan_lines):
        if not ln.strip():
            continue
        try:
            row = next(csv.reader([ln], delimiter=delimiter))
            field_counts.append((i, len(row)))
        except Exception:
            continue

    if not field_counts:
        log.append("Header detection: no parseable non-empty lines found.")
        return text, log, {}

    from collections import Counter
    counts_only = [c for _, c in field_counts]
    modal_fields = Counter(counts_only).most_common(1)[0][0]

    candidates = [(i, lines[i]) for i, c in field_counts if c == modal_fields]
    if not candidates:
        log.append("Header detection: no lines match modal field count.")
        return text, log, {}
    # Always include the first non-empty line as a candidate,
    # even if it does not match the modal field count
    first_non_empty = next((i for i, ln in enumerate(lines) if ln.strip()), None)
    if first_non_empty is not None and all(i != first_non_empty for i, _ in candidates):
        candidates.append((first_non_empty, lines[first_non_empty]))

    # Sort candidates by line index to keep selection stable
    candidates.sort(key=lambda x: x[0])
    import re

    def score_header_line(line: str) -> float:
        # quote-aware tokenization
        try:
            parts = next(csv.reader([line], delimiter=delimiter))
        except Exception:
            parts = line.split(delimiter)

        parts = [p.strip() for p in parts]
        n = len(parts) if parts else 1

        alpha_tokens = sum(1 for p in parts if re.search(r"[A-Za-z]", p))
        numeric_tokens = sum(1 for p in parts if re.fullmatch(r"[-+]?\d+(\.\d+)?", p))
        date_like = sum(1 for p in parts if re.match(r"\d{4}-\d{2}-\d{2}", p))

        uniq_ratio = len(set(parts)) / n
        avg_len = sum(len(p) for p in parts) / n

        score = 0.0
        score += (alpha_tokens / n) * 4.0
        score -= (numeric_tokens / n) * 5.0
        score -= (date_like / n) * 4.0
        score += uniq_ratio * 2.0
        score -= avg_len * 0.05

        if ":" in line and n <= 2:
            score -= 3.0

        return score

    scored = [(i, score_header_line(ln)) for i, ln in candidates]
    scored.sort(key=lambda x: x[1], reverse=True)

    best_i, best_score = scored[0]
    second_score = scored[1][1] if len(scored) > 1 else float("-inf")
    score_gap = best_score - second_score

    top3 = scored[:3]
    log.append("Header candidates (top 3): " + ", ".join([f"line {i+1} score={s:.2f}" for i, s in top3]))
    log.append(f"Header selected: line {best_i+1} (score={best_score:.2f})")
    log.append(f"Header score gap: {score_gap:.2f}")
    log.append(f"Header modal field count: {modal_fields}")

    MIN_GAP = 0.75
    MAX_PREAMBLE = 15

    if best_i > MAX_PREAMBLE:
        log.append(f"Header detection: candidate too deep (>{MAX_PREAMBLE}); leaving file unchanged.")
        return text, log, {"header_line_index": 0, "delimiter": delimiter, "column_count": modal_fields, "confidence_score": round(best_score, 2), "score_gap": round(score_gap, 2)}

    if best_i > 0 and score_gap < MIN_GAP:
        log.append("Header detection: low confidence (small score gap); leaving file unchanged.")
        return text, log, {"header_line_index": best_i, "delimiter": delimiter, "column_count": modal_fields, "confidence_score": round(best_score, 2), "score_gap": round(score_gap, 2)}

    if best_i > 0:
        log.append(f"Header detected on line {best_i+1}; removed {best_i} preamble line(s).")
    else:
        log.append("Header appears to be on the first line (no preamble removed).")

    cleaned_text = "\n".join(lines[best_i:])

    header_info = {
        "header_line_index": best_i,
        "delimiter": delimiter,
        "column_count": modal_fields,
        "confidence_score": round(best_score, 2),
        "score_gap": round(score_gap, 2),
    }

    return cleaned_text, log, header_info

def detect_delimiter(path: Path) -> tuple[str, list[str]]:
    log: list[str] = []
    sample = path.read_text(encoding="utf-8")[:8192]
    lines = [ln for ln in sample.splitlines() if ln.strip()][:20]
    sniff_sample = "\n".join(lines)

    try:
        dialect = csv.Sniffer().sniff(sniff_sample, delimiters=_CANDIDATE_DELIMS)
        delim = dialect.delimiter
        if delim in _CANDIDATE_DELIMS:
            log.append(f"Detected delimiter: {repr(delim)}.")
            return delim, log
    except Exception:
        pass

    best = ","
    best_score = -1.0
    for d in _CANDIDATE_DELIMS:
        counts = [ln.count(d) for ln in lines if ln]
        if not counts:
            continue
        avg = sum(counts) / len(counts)
        var = sum((c - avg) ** 2 for c in counts) / len(counts)
        score = avg - (var ** 0.5)
        if score > best_score:
            best_score = score
            best = d

    log.append(f"Detected delimiter: {repr(best)} (heuristic).")
    return best, log


def delimiter_label(d: str) -> str:
    return {"\t": "TAB", ",": "Comma", ";": "Semicolon", "|": "Pipe"}.get(d, d)


def cleaned_download_name(delim: str) -> str:
    return "cleaned.tsv" if delim == "\t" else "cleaned.csv"


def read_csv_lenient(path: Path, delimiter: str) -> tuple[pd.DataFrame, list[str], bool, list[int]]:
    import_log: list[str] = []
    rows: list[list[str]] = []

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f, delimiter=delimiter)
        for r in reader:
            rows.append(r)
            if len(rows) > (MAX_ROWS + 1):
                raise ValueError(f"Too many rows. Limit is {MAX_ROWS:,} data rows.")

    if not rows:
        raise ValueError("File appears empty or could not be parsed.")

    header = rows[0]
    n = len(header)
    if n > MAX_COLS:
        raise ValueError(f"Too many columns. Limit is {MAX_COLS:,} columns.")

    fixed_too_long = 0
    fixed_too_short = 0
    repaired_indices: list[int] = []

    cleaned_rows = [header]
    df_row_index = 0

    for r in rows[1:]:
        repaired = False
        if len(r) > n:
            r = r[:n]
            fixed_too_long += 1
            repaired = True
        elif len(r) < n:
            r = r + [""] * (n - len(r))
            fixed_too_short += 1
            repaired = True

        if repaired:
            repaired_indices.append(df_row_index)

        cleaned_rows.append(r)
        df_row_index += 1

    import_warning = (fixed_too_long > 0) or (fixed_too_short > 0)

    if fixed_too_long:
        import_log.append(f"Fixed {fixed_too_long:,} rows with extra columns (import repair).")
    if fixed_too_short:
        import_log.append(f"Fixed {fixed_too_short:,} rows with missing columns (import repair).")
    if not import_warning:
        import_log.append("Row structure was consistent (no import repairs needed).")

    df = pd.DataFrame(cleaned_rows[1:], columns=cleaned_rows[0])
    return df, import_log, import_warning, repaired_indices

def guess_delimiter_euro_aware(path: Path, candidates: list[str] | None = None) -> tuple[str, list[str]]:
    """
    Improved delimiter detection:
    - Uses csv.Sniffer when possible
    - If ambiguous, uses heuristics + 'decimal comma' detection:
        if semicolon looks consistent AND many fields look like '12,34' numbers,
        prefer ';' over ','.
    Returns (delimiter, log_lines)
    """
    log: list[str] = []
    if candidates is None:
        candidates = [",", ";", "\t", "|"]

    sample = path.read_text(encoding="utf-8")[:16384]
    lines = [ln for ln in sample.splitlines() if ln.strip()][:30]
    sniff_sample = "\n".join(lines)

    # 1) Try Sniffer first
    try:
        dialect = csv.Sniffer().sniff(sniff_sample, delimiters=candidates)
        d = dialect.delimiter
        if d in candidates:
            log.append(f"Detected delimiter: {repr(d)} (sniffer).")
            return d, log
    except Exception:
        pass

    # Helper: measure how consistent delimiter count is across lines
    def consistency_score(delim: str) -> float:
        counts = [ln.count(delim) for ln in lines]
        if not counts:
            return -1.0
        avg = sum(counts) / len(counts)
        var = sum((c - avg) ** 2 for c in counts) / len(counts)
        # prefer more separators, but penalize inconsistency
        return avg - (var ** 0.5)

    # Helper: detect decimal-comma numbers in tokens
    # Examples: 12,34  1.234,56  -12,34  (12,34)
    dec_comma_re = re.compile(r"^\(?-?\d{1,3}([.\s]\d{3})*,\d+\)?$")

    def decimal_comma_density(delim: str) -> float:
        tokens = []
        for ln in lines:
            parts = [p.strip().strip('"') for p in ln.split(delim)]
            tokens.extend(parts)
        if not tokens:
            return 0.0
        matches = sum(1 for t in tokens if dec_comma_re.match(t))
        return matches / len(tokens)

    scores = {d: consistency_score(d) for d in candidates}
    best = max(scores, key=scores.get)

    # 2) Euro-aware tie breaker between ',' and ';'
    # If ';' is close to ',' and decimal-comma tokens are common, prefer ';'
    if "," in scores and ";" in scores:
        euro_density_comma_split = decimal_comma_density(";")  # tokenizing by ';' shows decimal commas inside fields
        close = abs(scores[";"] - scores[","]) <= 0.35  # heuristic closeness threshold
        if close and euro_density_comma_split >= 0.06:  # 6%+ of tokens look like decimal-comma numbers
            log.append(f"Detected delimiter: ';' (euro-aware: decimal comma density {euro_density_comma_split:.1%}).")
            return ";", log

    log.append(f"Detected delimiter: {repr(best)} (heuristic).")
    return best, log

# ============================
# Cleaning
# ============================

def normalize_numeric_strings_df(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """
    Convert common money/number formats to numeric:
    - European: 1.234,56 -> 1234.56
    - US: 1,234.56 -> 1234.56
    - Parentheses negatives: (12,34) -> -12.34
    Only converts a column if conversion succeeds for most non-empty values.
    """
    log: list[str] = []
    converted_cols = 0

    euro_re = re.compile(r"^\(?-?\d{1,3}([.\s]\d{3})*,\d+\)?$")      # 1.234,56 / 1 234,56
    us_re   = re.compile(r"^\(?-?\d{1,3}(,\d{3})*(\.\d+)?\)?$")      # 1,234.56

    def to_float_safe(s: str) -> float | None:
        if s is None:
            return None
        t = str(s).strip()
        if not t:
            return None

        neg = False
        if t.startswith("(") and t.endswith(")"):
            neg = True
            t = t[1:-1].strip()

        # remove currency symbols and spaces around
        t = t.replace("$", "").replace("€", "").replace("£", "").strip()

        # Euro style: thousands '.' or space, decimal ','
        if euro_re.match(t):
            t = t.replace(" ", "")
            t = t.replace(".", "")
            t = t.replace(",", ".")
        # US style: thousands ',', decimal '.'
        elif us_re.match(t):
            t = t.replace(",", "")
        else:
            return None

        try:
            v = float(t)
            return -v if neg else v
        except Exception:
            return None

    for col in df.columns:
        s = df[col]
        if not (s.dtype == object or pd.api.types.is_string_dtype(s)):
            continue

        non_empty = s.dropna().map(lambda x: str(x).strip()).loc[lambda x: x != ""]
        if non_empty.empty:
            continue

        conv = non_empty.map(to_float_safe)
        success = conv.notna().mean()

        # Only convert if we are pretty confident this is a numeric column
        if success >= 0.85 and len(non_empty) >= 5:
            # Convert whole column (preserve blanks)
            new_col = s.map(lambda x: to_float_safe(x) if str(x).strip() != "" else None)
            df[col] = pd.to_numeric(new_col, errors="coerce")
            converted_cols += 1

    if converted_cols:
        log.append(f"Normalized numeric formats in {converted_cols} column(s) (EU/US separators, parentheses negatives).")
    else:
        log.append("Numeric normalization: no eligible numeric columns detected.")
    return df, log


def snake_case(name: str) -> str:
    s = str(name).strip().lower()
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"_+", "_", s)
    s = s.strip("_")
    return s or "col"


def normalize_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    log: list[str] = []
    original = list(df.columns)

    base_cols = [snake_case(c) for c in df.columns]
    seen: dict[str, int] = {}
    unique_cols: list[str] = []
    for c in base_cols:
        if c not in seen:
            seen[c] = 1
            unique_cols.append(c)
        else:
            seen[c] += 1
            unique_cols.append(f"{c}_{seen[c]}")

    if unique_cols != original:
        df.columns = unique_cols
        log.append("Normalized column names (consistent and import-friendly).")

    return df, log


def trim_whitespace_df(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    log: list[str] = []
    changed_total = 0
    text_cols = 0

    for col in df.columns:
        s = df[col]
        if s.dtype == object or pd.api.types.is_string_dtype(s):
            before = s.copy()
            after = before.map(lambda x: x.strip() if isinstance(x, str) else x)
            changed = int((before.ne(after) & before.notna()).sum())
            changed_total += changed
            text_cols += 1
            df[col] = after

    if text_cols == 0:
        log.append("No text columns found for whitespace cleanup.")
    elif changed_total == 0:
        log.append("No whitespace issues found in text cells.")
    else:
        log.append(f"Trimmed whitespace in {changed_total:,} text cells (prevents grouping/matching issues).")

    return df, log


def remove_duplicates_and_empty_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    log: list[str] = []

    before = len(df)
    df2 = df.dropna(how="all")
    removed_empty = before - len(df2)
    log.append(f"Removed {removed_empty:,} fully empty rows." if removed_empty else "No fully empty rows found.")
    df = df2

    before = len(df)
    df2 = df.drop_duplicates()
    removed_dupes = before - len(df2)
    log.append(f"Removed {removed_dupes:,} duplicate rows (exact matches)." if removed_dupes else "No exact duplicate rows found.")

    return df2, log


def clean_csv(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    changelog: list[str] = []
    df, log = normalize_columns(df); changelog += log
    df, log = trim_whitespace_df(df); changelog += log
    df, log = remove_duplicates_and_empty_rows(df); changelog += log
    return df, changelog


# ============================
# Near-duplicates
# ============================
def _should_ignore_col(col: str) -> bool:
    s = str(col).lower().strip()
    if s == "id" or s.endswith("_id") or s.startswith("id_") or "_id_" in s:
        return True
    if "uuid" in s or "guid" in s:
        return True
    if "transaction" in s or "txn" in s:
        return True
    if "reference" in s or s in {"ref", "reference"} or "ref_" in s or s.endswith("_ref"):
        return True
    if "date" in s or "time" in s or "timestamp" in s or s.endswith("_at"):
        return True
    if "balance" in s or "running_total" in s or "remaining" in s:
        return True
    return False


def _normalize_text_for_compare(x: Any) -> str:
    if x is None:
        return ""
    s = str(x)
    s = re.sub(r"\s+", " ", s)
    return s.strip().lower()


def _row_to_dict(df: pd.DataFrame, idx: int) -> dict:
    row = df.iloc[idx].to_dict()
    return {k: _json_safe(v) for k, v in row.items()}


def analyze_near_duplicates(df: pd.DataFrame, max_examples: int = 5) -> tuple[list[str], int, list[dict], pd.Series]:
    ignored_cols = [c for c in df.columns if _should_ignore_col(c)]
    compare_cols = [c for c in df.columns if c not in ignored_cols]

    if not compare_cols or df.empty:
        dup_mask = pd.Series([False] * len(df), index=df.index)
        return ignored_cols, 0, [], dup_mask

    comp = df[compare_cols].copy().fillna("")
    for c in comp.columns:
        if pd.api.types.is_string_dtype(comp[c]) or comp[c].dtype == object:
            comp[c] = comp[c].map(_normalize_text_for_compare)

    dup_mask = comp.duplicated(keep="first")
    dup_count = int(dup_mask.sum())

    examples: list[dict] = []
    if dup_count > 0:
        groups = comp.groupby(list(comp.columns), sort=False).indices
        for _, idxs in groups.items():
            if len(idxs) >= 2:
                kept = idxs[0]
                for rem in idxs[1:]:
                    examples.append({"kept": _row_to_dict(df, int(kept)), "removed": _row_to_dict(df, int(rem))})
                    if len(examples) >= max_examples:
                        break
            if len(examples) >= max_examples:
                break

    return ignored_cols, dup_count, examples, dup_mask


def render_near_dupe_compare_table(ex: dict, mode: str) -> str:
    removed_label = "Would remove" if mode == "preview" else "Removed"
    kept = dict(ex["kept"])
    removed = dict(ex["removed"])

    cols = list(kept.keys())
    for k in removed.keys():
        if k not in cols:
            cols.append(k)

    kept_row = {"status": "Kept", **{k: kept.get(k) for k in cols}}
    rem_row = {"status": removed_label, **{k: removed.get(k) for k in cols}}

    df = pd.DataFrame([kept_row, rem_row])
    return df.to_html(index=False, escape=True)


# ============================
# Previews
# ============================
def df_to_html_table(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "<p class='muted'>No rows to display.</p>"
    return df.to_html(index=False, escape=True)


def build_previews(df: pd.DataFrame, repaired_indices: list[int]) -> tuple[str, str, str]:
    first_df = df.head(10)
    last_df = df.tail(10)

    repaired_html = ""
    if repaired_indices:
        idx = repaired_indices[:10]
        idx = [i for i in idx if 0 <= i < len(df)]
        if idx:
            repaired_df = df.iloc[idx]
            repaired_html = df_to_html_table(repaired_df)

    return df_to_html_table(first_df), df_to_html_table(last_df), repaired_html


# ============================
# Payment helper
# ============================
def is_payment_pending(m: dict) -> bool:
    return bool(PAYMENTS_ENABLED and (not m.get("paid")) and m.get("stripe_session_id"))

# ============================
# Routes
# ============================

@app.get("/sitemap.xml")
def sitemap():
    pages = [
        "",
        "/problems",
        "/problems/expected-fields-error",
        "/problems/expected-fields-saw-fields",
        "/problems/excel-one-column-csv",
        "/problems/powerbi-decimal-comma-csv",
        "/problems/csv-encoding-utf8-windows-1252",
    ]

    xml = ['<?xml version="1.0" encoding="UTF-8"?>']
    xml.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')

    for p in pages:
        xml.append("  <url>")
        xml.append(f"    <loc>{BASE_URL}{p}</loc>")
        xml.append("  </url>")

    xml.append("</urlset>")
    return app.response_class("\n".join(xml), mimetype="application/xml")

@app.get("/problems")
def problems_index():
    return render_template_string(PROBLEMS_INDEX_HTML)

@app.get("/problems/csv-encoding-utf8-windows-1252")
def problem_csv_encoding():
    return render_template_string(PROBLEM_CSV_ENCODING_HTML)

@app.get("/problems/expected-fields-saw-fields")
def problem_expected_fields_saw_fields():
    return render_template_string(PROBLEM_EXPECTED_FIELDS_SAW_FIELDS_HTML)

@app.get("/problems/powerbi-decimal-comma-csv")
def problem_powerbi_decimal_comma():
    return render_template_string(PROBLEM_POWERBI_DECIMAL_COMMA_HTML)

@app.get("/problems/excel-one-column-csv")
def problem_excel_one_column():
    return render_template_string(PROBLEM_EXCEL_ONE_COLUMN_HTML)

@app.get("/problems/expected-fields-error")
def problem_expected_fields():
    return render_template_string(PROBLEM_EXPECTED_FIELDS_HTML)

@app.get("/favicon.ico")
def favicon():
    return ("", 204)

@app.get("/")
def index():
    cleanup_old_files()
    log_event("page_view_home", payments_enabled=PAYMENTS_ENABLED)
    return render_template_string(
        INDEX_HTML,
        error=None,
        max_mb=MAX_BYTES // (1024 * 1024),
        retention=RETENTION_MINUTES,
        payments_enabled=PAYMENTS_ENABLED,
        max_rows=MAX_ROWS,
        max_cols=MAX_COLS,
        support_email=SUPPORT_EMAIL,
    )


@app.post("/upload")
def upload():
    cleanup_old_files()

    if bytes_too_large(request):
        log_event("upload_rejected_file_too_large", file_bytes=request.content_length)
        return render_template_string(
            INDEX_HTML,
            error=f"File too large. Max is {MAX_BYTES // (1024 * 1024)} MB.",
            max_mb=MAX_BYTES // (1024 * 1024),
            retention=RETENTION_MINUTES,
            payments_enabled=PAYMENTS_ENABLED,
            max_rows=MAX_ROWS,
            max_cols=MAX_COLS,
            support_email=SUPPORT_EMAIL,
        ), 413

    ip = get_client_ip()
    if not rate_limit_check(ip):
        log_event("upload_rate_limited")
        return render_template_string(
            INDEX_HTML,
            error=f"Rate limit: too many uploads. Please wait {RATE_WINDOW_SECONDS} seconds and try again.",
            max_mb=MAX_BYTES // (1024 * 1024),
            retention=RETENTION_MINUTES,
            payments_enabled=PAYMENTS_ENABLED,
            max_rows=MAX_ROWS,
            max_cols=MAX_COLS,
            support_email=SUPPORT_EMAIL,
        ), 429

    f = request.files.get("file")
    if not f or not f.filename:
        log_event("upload_missing_file")
        abort(400)

    original_filename = f.filename
    if not looks_like_csv_name(original_filename):
        log_event("upload_rejected_extension", filename=original_filename)
        return render_template_string(
            INDEX_HTML,
            error="Please upload a .csv or .tsv file (a .txt export is also OK).",
            max_mb=MAX_BYTES // (1024 * 1024),
            retention=RETENTION_MINUTES,
            payments_enabled=PAYMENTS_ENABLED,
            max_rows=MAX_ROWS,
            max_cols=MAX_COLS,
            support_email=SUPPORT_EMAIL,
        ), 400

    near_preview = bool(request.form.get("near_dupes_preview"))
    near_remove = bool(request.form.get("near_dupes_remove"))
    if near_remove:
        near_preview = True
        
    normalize_numbers = bool(request.form.get("normalize_numbers"))
    
    job_id = uuid.uuid4().hex
    rp = raw_path(job_id)
    np = norm_path(job_id)
    op = out_path(job_id)

    log_event(
        "upload_start",
        job_id=job_id,
        filename=original_filename,
        file_bytes=request.content_length,
        near_preview=near_preview,
        near_remove=near_remove,
        normalize_numbers=normalize_numbers,
    )

    f.save(rp)

    if not looks_like_text_file(rp):
        log_event("upload_rejected_binary", job_id=job_id)
        rp.unlink(missing_ok=True)
        return render_template_string(
            INDEX_HTML,
            error="That file doesn't look like a text CSV/TSV (binary data detected).",
            max_mb=MAX_BYTES // (1024 * 1024),
            retention=RETENTION_MINUTES,
            payments_enabled=PAYMENTS_ENABLED,
            max_rows=MAX_ROWS,
            max_cols=MAX_COLS,
            support_email=SUPPORT_EMAIL,
        ), 400

    # Decode + normalize to UTF-8 + quote stitching
    parse_path, structural_log, encoding_used, stitch_stats = normalize_to_utf8_lf(rp, np)

    log_event("upload_decoded", job_id=job_id, encoding=encoding_used, **stitch_stats)

    # Detect delimiter (EU-aware)
    delim, delim_log = guess_delimiter_euro_aware(parse_path)

    # Header detection operates on TEXT, not Path
    text = parse_path.read_text(encoding="utf-8")
    text, header_log, header_info = detect_and_strip_preamble(text, delimiter=delim)
    parse_path.write_text(text, encoding="utf-8", newline="\n")

    # Split header log into user-facing vs diagnostics
    DEBUG_PREFIXES = (
        "Header candidates",
        "Header selected",
        "Header score gap",
        "Header modal field count",
    )
    header_user_log = [x for x in header_log if not x.startswith(DEBUG_PREFIXES)]
    header_debug_log = [x for x in header_log if x.startswith(DEBUG_PREFIXES)]

    # Log header detection (full detail goes to server logs)
    log_event("header_detection", job_id=job_id, note="; ".join(header_log) if header_log else "")

    # Lenient parse (pads/truncates rows)
    try:
        df, import_log, import_warning, repaired_indices = read_csv_lenient(parse_path, delimiter=delim)
    except ValueError as e:
        log_event("upload_rejected_limits_or_parse", job_id=job_id, error=str(e), delimiter=delim)
        rp.unlink(missing_ok=True)
        np.unlink(missing_ok=True)
        return render_template_string(
            INDEX_HTML,
            error=str(e),
            max_mb=MAX_BYTES // (1024 * 1024),
            retention=RETENTION_MINUTES,
            payments_enabled=PAYMENTS_ENABLED,
            max_rows=MAX_ROWS,
            max_cols=MAX_COLS,
            support_email=SUPPORT_EMAIL,
        ), 400

    # Lenient parse (pads/truncates rows)
    try:
        df, import_log, import_warning, repaired_indices = read_csv_lenient(parse_path, delimiter=delim)
    except ValueError as e:
        log_event("upload_rejected_limits_or_parse", job_id=job_id, error=str(e), delimiter=delim)
        rp.unlink(missing_ok=True)
        np.unlink(missing_ok=True)
        return render_template_string(
            INDEX_HTML,
            error=str(e),
            max_mb=MAX_BYTES // (1024 * 1024),
            retention=RETENTION_MINUTES,
            payments_enabled=PAYMENTS_ENABLED,
            max_rows=MAX_ROWS,
            max_cols=MAX_COLS,
            support_email=SUPPORT_EMAIL,
        ), 400

    # Clean
    df2, clean_log = clean_csv(df)
    
    if normalize_numbers:
        df2, num_log = normalize_numeric_strings_df(df2)
        clean_log += num_log
        log_event("numeric_normalization", job_id=job_id, notes=num_log)
    
    DEBUG_PREFIXES = (
        "Header candidates",
        "Header selected",
        "Header score gap",
        "Header modal field count",
    )

    header_user_log = [x for x in header_log if not x.startswith(DEBUG_PREFIXES)]
    header_debug_log = [x for x in header_log if x.startswith(DEBUG_PREFIXES)]

    changelog = structural_log + delim_log + header_user_log + import_log + clean_log

    # Near-dupes (optional)
    near_dupes_mode = ""
    ignored_cols: list[str] = []
    near_dupes_count = 0
    near_dupe_examples_rows: list[dict] = []
    near_dupe_examples_tables: list[str] = []

    if near_preview:
        ignored_cols, near_dupes_count, near_dupe_examples_rows, dup_mask = analyze_near_duplicates(df2, max_examples=5)

        if near_dupes_count == 0:
            near_dupes_mode = "remove" if near_remove else "preview"
            changelog.append("No near-duplicate rows found (using the near-duplicate rules).")
        else:
            if near_remove:
                df2 = df2.loc[~dup_mask].copy()
                near_dupes_mode = "remove"
                changelog.append(f"Removed {near_dupes_count:,} near-duplicate rows.")
            else:
                near_dupes_mode = "preview"
                changelog.append(f"Dry run: {near_dupes_count:,} near-duplicate rows would be removed.")

        changelog.append(
            "Near-duplicate rule: compare all columns except "
            + (", ".join(ignored_cols) if ignored_cols else "(none)")
            + "."
        )
        near_dupe_examples_tables = [render_near_dupe_compare_table(ex, near_dupes_mode) for ex in near_dupe_examples_rows]

    # Write output
    df2.to_csv(op, index=False, encoding="utf-8", lineterminator="\n", sep=delim)
    changelog.append(f"Wrote output as UTF-8 with standard newlines using delimiter {repr(delim)}.")
    np.unlink(missing_ok=True)

    rows, cols = int(df2.shape[0]), int(df2.shape[1])
    preview_first, preview_last, preview_repaired = build_previews(df2, repaired_indices)

    write_manifest(
        job_id,
        {
            "paid": False,
            "rows": rows,
            "cols": cols,
            "changelog": changelog,
            "import_warning": import_warning,
            "repaired_row_indices": repaired_indices,
            "near_dupes_mode": near_dupes_mode,
            "ignored_cols": ignored_cols,
            "near_dupes_count": near_dupes_count,
            "near_dupe_examples_rows": near_dupe_examples_rows,
            "detected_delimiter": delim,
            "original_file": rp.name,
            "original_filename": original_filename or "original.csv",
        },
    )

    log_event("upload_complete", job_id=job_id, delimiter=delim, rows=rows, cols=cols, near_dupes_mode=near_dupes_mode)

    return render_template_string(
        RESULT_HTML,
        job_id=job_id,
        rows=rows,
        cols=cols,
        changelog=changelog,
        import_warning=import_warning,
        near_dupes_mode=near_dupes_mode,
        ignored_cols=ignored_cols,
        near_dupe_examples=near_dupe_examples_tables,
        preview_first=preview_first,
        preview_last=preview_last,
        preview_repaired=preview_repaired,
        retention=RETENTION_MINUTES,
        paid=False,
        payments_enabled=PAYMENTS_ENABLED,
        detected_delimiter=delim,
        detected_delimiter_label=delimiter_label(delim),
        payment_pending=False,
        support_email=SUPPORT_EMAIL,
    )


@app.get("/result/<job_id>")
def result(job_id: str):
    cleanup_old_files()

    m = read_manifest(job_id)
    op = out_path(job_id)
    if not m or not op.exists():
        log_event("result_not_found", job_id=job_id)
        abort(404)

    delim = m.get("detected_delimiter") or ","

    # Detected formats (for badges)
    encoding_label = "UTF-8"  # because we normalize to UTF-8 in the pipeline
    numbers_label = "Normalized" if any("Normalized numeric formats" in c for c in m.get("changelog", [])) else "Unchanged"
    header_label = "Auto-detected" if any("Header detected" in c for c in m.get("changelog", [])) else "First row"
    try:
        df = pd.read_csv(op, encoding="utf-8", sep=delim)
    except Exception:
        df = pd.DataFrame()

    repaired_indices = m.get("repaired_row_indices", []) or []
    preview_first, preview_last, preview_repaired = build_previews(df, repaired_indices)

    near_dupes_mode = (m.get("near_dupes_mode") or "").strip()
    ignored_cols = m.get("ignored_cols", []) or []
    examples_rows = m.get("near_dupe_examples_rows", []) or []
    near_dupe_examples_tables: List[str] = []
    if near_dupes_mode and examples_rows:
        near_dupe_examples_tables = [render_near_dupe_compare_table(ex, near_dupes_mode) for ex in examples_rows]

    payment_pending = is_payment_pending(m)

    return render_template_string(
        RESULT_HTML,
        job_id=job_id,
        rows=m.get("rows"),
        cols=m.get("cols"),
        changelog=m.get("changelog", []),
        import_warning=bool(m.get("import_warning")),
        near_dupes_mode=near_dupes_mode,
        ignored_cols=ignored_cols,
        near_dupe_examples=near_dupe_examples_tables,
        preview_first=preview_first,
        preview_last=preview_last,
        preview_repaired=preview_repaired,
        retention=RETENTION_MINUTES,
        paid=bool(m.get("paid")),
        payments_enabled=PAYMENTS_ENABLED,
        detected_delimiter=delim,
        detected_delimiter_label=delimiter_label(delim),
        encoding_label=encoding_label,
        numbers_label=numbers_label,
        header_label=header_label,
        payment_pending=payment_pending,
        support_email=SUPPORT_EMAIL,
    )


@app.get("/download_original/<job_id>")
def download_original(job_id: str):
    cleanup_old_files()
    m = read_manifest(job_id)
    if not m:
        abort(404)

    p = WORK_DIR / str(m.get("original_file") or "")
    if not p.exists():
        abort(404)

    return send_file(p, as_attachment=True, download_name=m.get("original_filename") or "original.csv")


@app.get("/download/<job_id>")
def download(job_id: str):
    cleanup_old_files()
    m = read_manifest(job_id)
    op = out_path(job_id)
    if not m or not op.exists():
        abort(404)

    delim = m.get("detected_delimiter") or ","

    if not PAYMENTS_ENABLED:
        return send_file(op, as_attachment=True, download_name=cleaned_download_name(delim))

    # If a payment session exists but we aren't marked paid yet, avoid bouncing to Stripe again.
    if is_payment_pending(m):
        return redirect(f"/result/{job_id}", code=303)

    if not m.get("paid"):
        return redirect(f"/pay/{job_id}", code=303)

    return send_file(op, as_attachment=True, download_name=cleaned_download_name(delim))


@app.get("/pay/<job_id>")
def pay(job_id: str):
    cleanup_old_files()

    if not PAYMENTS_ENABLED:
        return redirect(f"/download/{job_id}", code=303)

    m = read_manifest(job_id)
    op = out_path(job_id)
    if not m or not op.exists():
        abort(404)

    session = stripe.checkout.Session.create(
        mode="payment",
        line_items=[{"price": PRICE_ID, "quantity": 1}],
        success_url=f"{BASE_URL}/success?job_id={job_id}&session_id={{CHECKOUT_SESSION_ID}}",
        cancel_url=f"{BASE_URL}/cancel?job_id={job_id}",
        metadata={"job_id": job_id},
    )

    # Persist session ID for \"pending\" UX
    m["stripe_session_id"] = session.get("id")
    manifest_path(job_id).write_text(json.dumps(m, indent=2), encoding="utf-8")

    return redirect(session.url, code=303)


@app.get("/success")
def success():
    cleanup_old_files()

    if not PAYMENTS_ENABLED:
        abort(404)

    job_id = (request.args.get("job_id") or "").strip()
    session_id = (request.args.get("session_id") or "").strip()
    if not job_id or not session_id:
        abort(400)

    sess = stripe.checkout.Session.retrieve(session_id)
    if sess.payment_status == "paid" and (sess.metadata or {}).get("job_id") == job_id:
        mark_paid(job_id, session_id=session_id)
        return render_template_string(
            SUCCESS_HTML,
            job_id=job_id,
            retention=RETENTION_MINUTES,
            support_email=SUPPORT_EMAIL,
        )

    return "Payment not confirmed.", 402


@app.get("/cancel")
def cancel():
    job_id = (request.args.get("job_id") or "").strip()
    return render_template_string(CANCEL_HTML, job_id=job_id)


@app.post("/stripe/webhook")
def stripe_webhook():
    if not STRIPE_WEBHOOK_SECRET:
        log_event("webhook_missing_secret")
        return ("Webhook secret not configured", 400)

    payload = request.get_data(as_text=False)
    sig_header = request.headers.get("Stripe-Signature", "")

    try:
        event = stripe.Webhook.construct_event(payload=payload, sig_header=sig_header, secret=STRIPE_WEBHOOK_SECRET)
    except ValueError:
        log_event("webhook_invalid_payload")
        return ("Invalid payload", 400)
    except stripe.error.SignatureVerificationError:
        log_event("webhook_invalid_signature")
        return ("Invalid signature", 400)

    event_id = event.get("id")
    event_type = event.get("type")
    log_event("webhook_received", stripe_event_id=event_id, stripe_event_type=event_type)

    if event_type == "checkout.session.completed":
        session = event["data"]["object"]
        metadata = session.get("metadata") or {}
        job_id = metadata.get("job_id")
        session_id = session.get("id")

        if job_id:
            m = read_manifest(job_id)
            if m.get("stripe_event_id") == event_id:
                log_event("webhook_duplicate_ignored", job_id=job_id, stripe_event_id=event_id)
                return ("ok", 200)
            mark_paid(job_id, session_id=session_id, event_id=event_id)
            log_event("webhook_marked_paid", job_id=job_id, stripe_session_id=session_id, stripe_event_id=event_id)

    return ("ok", 200)


if __name__ == "__main__":
    debug = os.environ.get("FLASK_DEBUG", "").lower() in {"1", "true", "yes"}
    app.run(host="127.0.0.1", port=5000, debug=debug)