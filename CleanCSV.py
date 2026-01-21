from __future__ import annotations

import csv
import json
import os
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, List

import pandas as pd
import stripe
from flask import Flask, abort, redirect, render_template_string, request, send_file

app = Flask(__name__)

# ============================
# Config
# ============================
WORK_DIR = Path(os.environ.get("CLEANCCSV_WORK_DIR", "/tmp")) / "cleancsv"
WORK_DIR.mkdir(parents=True, exist_ok=True)

MAX_BYTES = int(os.environ.get("CLEANCCSV_MAX_BYTES", str(20 * 1024 * 1024)))
RETENTION_MINUTES = int(os.environ.get("CLEANCCSV_RETENTION_MINUTES", "30"))

stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")
PRICE_ID = os.environ.get("STRIPE_PRICE_ID", "")
BASE_URL = os.environ.get("APP_BASE_URL", "http://127.0.0.1:5000")
PAYMENTS_ENABLED = bool(stripe.api_key and PRICE_ID)

# ============================
# HTML
# ============================

INDEX_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>CleanCSV</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    body { font-family: system-ui, sans-serif; margin: 40px; max-width: 900px; }
    .card { border: 1px solid #ddd; border-radius: 12px; padding: 20px; }
    .btn { padding: 10px 14px; border-radius: 10px; border: 1px solid #111; background: #111; color: #fff; text-decoration: none; }
    .btn.secondary { background: #fff; color: #111; }
    .muted { color: #666; font-size: 14px; }
  </style>
</head>
<body>
<h1>CleanCSV</h1>
<p class="muted">Fix CSV files that won’t import — preview exactly what was repaired before you download.</p>

<div class="card">
  <form action="/upload" method="post" enctype="multipart/form-data">
    <input type="file" name="file" required /><br><br>

    <label>
      <input type="checkbox" name="near_dupes_preview" />
      Preview near-duplicates
    </label><br>

    <label>
      <input type="checkbox" name="near_dupes_remove" />
      Remove near-duplicates
    </label><br><br>

    <button class="btn">Upload CSV</button>
  </form>

  <p class="muted">Max file size: {{ max_mb }} MB • Files auto-delete after {{ retention }} minutes</p>
</div>
</body>
</html>
"""

RESULT_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Results</title>
  <style>
    body { font-family: system-ui, sans-serif; margin: 40px; max-width: 1100px; }
    .btn { padding: 10px 14px; border-radius: 10px; border: 1px solid #111; background: #111; color: #fff; text-decoration: none; }
    .btn.secondary { background: #fff; color: #111; }
    .card { border: 1px solid #ddd; border-radius: 12px; padding: 20px; margin-top: 16px; }
    .muted { color: #666; font-size: 14px; }
  </style>
</head>
<body>

<h1>Results</h1>

<div class="card">
  <p><b>Rows:</b> {{ rows }} &nbsp; <b>Columns:</b> {{ cols }}</p>

  <ul>
    {% for item in changelog %}
      <li>{{ item }}</li>
    {% endfor %}
  </ul>

  <p>
    <a class="btn" href="/download/{{ job_id }}">
      {% if paid or not payments_enabled %}
        Download cleaned file
      {% else %}
        Pay $5 & download
      {% endif %}
    </a>

    <a class="btn secondary" href="/download_original/{{ job_id }}">
      Download original
    </a>
  </p>

  <p class="muted">
    $5 one-time download • Files are automatically deleted after 30 minutes
  </p>
</div>

</body>
</html>
"""

SUCCESS_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Payment received</title>
  <style>
    body { font-family: system-ui, sans-serif; margin: 40px; }
    .btn { padding: 10px 14px; border-radius: 10px; border: 1px solid #111; background: #111; color: #fff; text-decoration: none; }
    .muted { color: #666; font-size: 14px; }
  </style>
</head>
<body>

<h1>Payment received</h1>

<p>
  <a class="btn" href="/download/{{ job_id }}">Download cleaned file</a>
</p>

<p class="muted">
  Need help? Email youremail@example.com and include Job ID: {{ job_id }}
</p>

</body>
</html>
"""

# ============================
# Helpers
# ============================

def cleanup_old_files():
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=RETENTION_MINUTES)
    for p in WORK_DIR.glob("*"):
        try:
            if datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc) < cutoff:
                p.unlink(missing_ok=True)
        except Exception:
            pass

def out_path(job_id): return WORK_DIR / f"{job_id}.clean.csv"
def raw_path(job_id): return WORK_DIR / f"{job_id}.raw"
def manifest_path(job_id): return WORK_DIR / f"{job_id}.json"

def write_manifest(job_id, data):
    manifest_path(job_id).write_text(json.dumps(data), encoding="utf-8")

def read_manifest(job_id):
    p = manifest_path(job_id)
    return json.loads(p.read_text()) if p.exists() else {}

# ============================
# Routes
# ============================

@app.route("/")
def index():
    cleanup_old_files()
    return render_template_string(
        INDEX_HTML,
        max_mb=MAX_BYTES // (1024 * 1024),
        retention=RETENTION_MINUTES,
    )

@app.route("/upload", methods=["POST"])
def upload():
    cleanup_old_files()

    f = request.files.get("file")
    if not f:
        abort(400)

    job_id = uuid.uuid4().hex
    raw = raw_path(job_id)
    out = out_path(job_id)

    f.save(raw)

    df = pd.read_csv(raw, engine="python")
    df.to_csv(out, index=False)

    write_manifest(job_id, {
        "paid": False,
        "rows": len(df),
        "cols": len(df.columns),
        "changelog": ["File uploaded successfully"]
    })

    return render_template_string(
        RESULT_HTML,
        job_id=job_id,
        rows=len(df),
        cols=len(df.columns),
        changelog=["File uploaded successfully"],
        paid=False,
        payments_enabled=PAYMENTS_ENABLED,
    )

@app.route("/download/<job_id>")
def download(job_id):
    cleanup_old_files()
    m = read_manifest(job_id)
    out = out_path(job_id)

    if PAYMENTS_ENABLED and not m.get("paid"):
        return redirect(f"/pay/{job_id}")

    return send_file(out, as_attachment=True)

@app.route("/download_original/<job_id>")
def download_original(job_id):
    cleanup_old_files()
    return send_file(raw_path(job_id), as_attachment=True)

@app.route("/pay/<job_id>")
def pay(job_id):
    session = stripe.checkout.Session.create(
        mode="payment",
        line_items=[{"price": PRICE_ID, "quantity": 1}],
        success_url=f"{BASE_URL}/success?job_id={job_id}&session_id={{CHECKOUT_SESSION_ID}}",
        cancel_url=f"{BASE_URL}/",
    )
    return redirect(session.url)

@app.route("/success")
def success():
    job_id = request.args.get("job_id")
    session_id = request.args.get("session_id")
    if not job_id or not session_id:
        abort(400)

    sess = stripe.checkout.Session.retrieve(session_id)
    if sess.payment_status == "paid":
        m = read_manifest(job_id)
        m["paid"] = True
        write_manifest(job_id, m)
        return render_template_string(SUCCESS_HTML, job_id=job_id)

    abort(402)

if __name__ == "__main__":
    app.run(debug=False)