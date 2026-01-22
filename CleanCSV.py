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
# Logging
# ============================
LOG_LEVEL = os.environ.get("CLEANCCSV_LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), format="%(message)s")
logger = logging.getLogger("cleancsv")


def get_client_ip() -> str:
    xff = request.headers.get("X-Forwarded-For", "")
    if xff:
        return xff.split(",")[0].strip()
    return request.remote_addr or "unknown"


def ip_hash(ip: str) -> str:
    return hashlib.sha256(ip.encode("utf-8")).hexdigest()[:12]


def log_event(event: str, **fields: Any) -> None:
    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        "service": "cleancsv",
    }
    try:
        payload["path"] = request.path
        payload["method"] = request.method
        payload["ua"] = request.headers.get("User-Agent", "")
        payload["ip_hash"] = ip_hash(get_client_ip())
    except Exception:
        pass

    for k, v in fields.items():
        payload[k] = v

    logger.info(json.dumps(payload, default=str))


# ============================
# Config
# ============================
WORK_DIR = Path(os.environ.get("CLEANCCSV_WORK_DIR", "/tmp")) / "cleancsv"
WORK_DIR.mkdir(parents=True, exist_ok=True)

MAX_BYTES = int(os.environ.get("CLEANCCSV_MAX_BYTES", str(20 * 1024 * 1024)))  # 20 MB default
RETENTION_MINUTES = int(os.environ.get("CLEANCCSV_RETENTION_MINUTES", "30"))

# Abuse / safety limits
MAX_ROWS = int(os.environ.get("CLEANCCSV_MAX_ROWS", "200000"))  # data rows (not counting header)
MAX_COLS = int(os.environ.get("CLEANCCSV_MAX_COLS", "300"))

RATE_WINDOW_SECONDS = int(os.environ.get("CLEANCCSV_RATE_WINDOW_SECONDS", "60"))
RATE_MAX_UPLOADS = int(os.environ.get("CLEANCCSV_RATE_MAX_UPLOADS", "10"))

# Stripe
stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")
PRICE_ID = os.environ.get("STRIPE_PRICE_ID", "")
BASE_URL = os.environ.get("APP_BASE_URL", "http://127.0.0.1:5000")

STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")

PAYMENTS_ENABLED = bool(stripe.api_key and PRICE_ID)

SUPPORT_EMAIL = "carney.christopher22@gmail.com"

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
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 40px; max-width: 920px; }
    .card { border: 1px solid #ddd; border-radius: 12px; padding: 20px; }
    .btn { display:inline-block; padding: 10px 14px; border-radius: 10px; border: 1px solid #111; background: #111; color: #fff; cursor: pointer; }
    input[type=file]{ margin: 12px 0; }
    .muted { color: #666; font-size: 14px; }
    .error { color: #b00020; white-space: pre-wrap; }
    .opt { margin-top: 14px; }
    label { cursor: pointer; }
    .small { font-size: 13px; color: #666; }
    .indent { margin-left: 22px; margin-top: 6px; }
  </style>
</head>
<body>
  <h1>CleanCSV</h1>
  <p class="muted">Fix CSV files that won’t import — preview exactly what was repaired before you download.</p>

  {% if error %}
    <p class="error"><b>Error:</b> {{ error }}</p>
  {% endif %}

  <div class="card">
    <form action="/upload" method="post" enctype="multipart/form-data">
      <input type="file" name="file" required accept=".csv,.tsv,.txt,text/csv,text/tab-separated-values,text/plain" />

      <div class="opt">
        <label>
          <input type="checkbox" name="near_dupes_preview" value="1" />
          <b>Preview</b> near-duplicates (dry run)
        </label>
        <div class="small indent">
          Shows examples and how many rows would be removed, but does not delete anything.
        </div>
      </div>

      <div class="opt">
        <label>
          <input type="checkbox" name="near_dupes_remove" value="1" />
          <b>Remove</b> near-duplicates
        </label>
        <div class="small indent">
          Removes rows that are identical under a strict comparison rule after ignoring ID/date/balance-style columns.
        </div>
      </div>

      <br />
      <button class="btn" type="submit">Upload file</button>
    </form>

    <p class="muted">Max file size: {{ max_mb }} MB • Files auto-delete after {{ retention }} minutes.</p>
    <p class="muted">Limits: {{ max_rows }} rows • {{ max_cols }} columns • {{ rate_max }} uploads per {{ rate_window }}s per IP.</p>
    {% if payments_enabled %}
      <p class="muted">Payment: enabled (pay-to-download).</p>
    {% else %}
      <p class="muted">Payment: disabled (missing Stripe env vars). Downloads will be free.</p>
    {% endif %}
  </div>
</body>
</html>
"""

RESULT_HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>CleanCSV - Results</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 40px; max-width: 1120px; }
    .card { border: 1px solid #ddd; border-radius: 12px; padding: 20px; margin-top: 16px; }
    .btn { display:inline-block; padding: 10px 14px; border-radius: 10px; border: 1px solid #111; background: #111; color: #fff; text-decoration:none; margin-right:10px; }
    .btn.secondary { background:#fff; color:#111; }
    .muted { color: #666; font-size: 14px; }
    ul { margin: 8px 0 0 18px; }
    .pill { display:inline-block; padding:4px 10px; border:1px solid #ddd; border-radius:999px; font-size:12px; color:#333; }
    .warn { color:#8a6d3b; }
    .section-title { margin: 0 0 8px; }

    .chip { display:inline-block; padding:4px 10px; border:1px solid #ddd; border-radius:999px; font-size:12px; margin: 4px 6px 0 0; }
    .chips { margin-top: 6px; }

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
    .table-wrap th, .table-wrap td {
      border-bottom: 1px solid #eee;
      padding: 8px;
      text-align: left;
      font-size: 13px;
      white-space: nowrap;
    }

    .subhead { font-weight: 600; margin: 0 0 6px; }
    .compare-note { margin: 0 0 10px; }

    .callout {
      border: 1px solid #e5e7eb;
      border-radius: 12px;
      padding: 12px;
      background: #fafafa;
      margin-top: 12px;
    }
  </style>
</head>
<body>
  <h1>Results</h1>
  <p class="muted">
    Job ID: {{ job_id }}
    &nbsp; <span class="pill">Paid: {{ "yes" if paid else "no" }}</span>
    {% if import_warning %}&nbsp; <span class="pill warn">Import repaired</span>{% endif %}
    {% if near_dupes_mode %}&nbsp; <span class="pill">Near-dupes: {{ near_dupes_mode }}</span>{% endif %}
    {% if detected_delimiter %}&nbsp; <span class="pill">Delimiter: {{ detected_delimiter_label }}</span>{% endif %}
  </p>

  <div class="card">
    <p><b>Rows:</b> {{ rows }} &nbsp; <b>Columns:</b> {{ cols }}</p>

    {% if payment_pending %}
      <div class="callout">
        <p class="muted" style="margin:0;">
          <b>Payment pending.</b> If you just paid, the webhook may take a few seconds. Refresh in ~5 seconds.
        </p>
        <p style="margin:10px 0 0;">
          <a class="btn secondary" href="/result/{{ job_id }}">Refresh status</a>
        </p>
      </div>
    {% endif %}

    {% if import_warning %}
      <p class="warn"><b>Note:</b> Some rows would have broken imports (wrong number of columns). We repaired them to match the header.</p>
    {% endif %}

    {% if near_dupes_mode %}
      <p class="subhead" style="margin-top:10px;">Near-duplicate comparison rule</p>
      <p class="muted" style="margin:0;">We compare all columns <b>except</b> the ones below.</p>
      <div class="chips">
        {% for c in ignored_cols %}
          <span class="chip">{{ c }}</span>
        {% endfor %}
        {% if ignored_cols|length == 0 %}
          <span class="chip">(none)</span>
        {% endif %}
      </div>
    {% endif %}

    <p class="muted" style="margin-top: 12px;"><b>Changes:</b></p>
    <ul>
      {% for item in changelog %}
        <li>{{ item }}</li>
      {% endfor %}
    </ul>

    <p style="margin-top: 16px;">
      <a class="btn" href="/download/{{ job_id }}">
        {% if paid or not payments_enabled %}
          Download cleaned file
        {% else %}
          Pay $5 & download
        {% endif %}
      </a>
      <a class="btn secondary" href="/download_original/{{ job_id }}">Download original</a>
      <a class="btn secondary" href="/result/{{ job_id }}">Results link</a>
    </p>

    <p class="muted">
      $5 one-time download • Files are automatically deleted after 30 minutes
    </p>
  </div>

  {% if near_dupe_examples %}
    <div class="card">
      <p class="section-title"><b>Near-duplicate examples</b></p>
      <p class="muted compare-note">
        Each example is shown as two rows in one table. “Kept” is the first occurrence; the other row is
        {{ "what would be removed" if near_dupes_mode == "preview" else "what was removed" }}.
      </p>

      {% for ex in near_dupe_examples %}
        <div style="margin-top: 14px;">
          <div class="table-wrap">{{ ex|safe }}</div>
        </div>
      {% endfor %}
    </div>
  {% endif %}

  <div class="card">
    <p class="section-title"><b>Preview: first 10 rows</b></p>
    <div class="table-wrap">{{ preview_first|safe }}</div>
  </div>

  <div class="card">
    <p class="section-title"><b>Preview: last 10 rows</b></p>
    <div class="table-wrap">{{ preview_last|safe }}</div>
  </div>

  {% if preview_repaired %}
    <div class="card">
      <p class="section-title"><b>Preview: repaired rows (up to 10)</b></p>
      <p class="muted">These rows had the wrong number of columns in the original file and were repaired to match the header.</p>
      <div class="table-wrap">{{ preview_repaired|safe }}</div>
    </div>
  {% endif %}
</body>
</html>
"""

SUCCESS_HTML = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>CleanCSV - Payment received</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 40px; max-width: 820px; }}
    .btn {{ display:inline-block; padding: 10px 14px; border-radius: 10px; border: 1px solid #111; background: #111; color: #fff; text-decoration:none; }}
    .muted {{ color: #666; font-size: 14px; }}
  </style>
</head>
<body>
  <h1>Payment received</h1>
  <p class="muted">You're good to go.</p>
  <p><a class="btn" href="/download/{{{{ job_id }}}}">Download cleaned file</a></p>
  <p class="muted">Need help? Email {SUPPORT_EMAIL} and include Job ID: {{{{ job_id }}}}</p>
  <p class="muted"><a href="/result/{{{{ job_id }}}}">Back to results</a></p>
</body>
</html>
"""

CANCEL_HTML = """
<!doctype html>
<html>
<head><meta charset="utf-8" /><title>CleanCSV - Payment canceled</title></head>
<body style="font-family:system-ui,sans-serif;margin:40px;">
  <h1>Payment canceled</h1>
  <p><a href="/">Back to upload</a></p>
</body>
</html>
"""

# ============================
# Abuse helpers
# ============================
_upload_hits = defaultdict(deque)  # ip -> deque[timestamps]


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
# File helpers
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
        "created_at": datetime.now(timezone.utc).isoformat(),
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
    if not m:
        return
    if m.get("paid"):
        return
    m["paid"] = True
    m["paid_at"] = datetime.now(timezone.utc).isoformat()
    if session_id:
        m["stripe_session_id"] = session_id
    if event_id:
        m["stripe_event_id"] = event_id
    write_manifest(job_id, m)


# ============================
# Newline normalization
# ============================
def normalize_line_endings_to_lf(src: Path, dst: Path) -> tuple[Path, list[str]]:
    log: list[str] = []
    data = src.read_bytes()
    if b"\r" not in data:
        return src, log
    normalized = data.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    if normalized != data:
        dst.write_bytes(normalized)
        log.append("Normalized line endings (fixed Windows/Mac-style newlines).")
        return dst, log
    return src, log


# ============================
# Delimiter detection + lenient parsing
# ============================
_CANDIDATE_DELIMS = [",", ";", "\t", "|"]


def detect_delimiter(path: Path) -> tuple[str, list[str]]:
    log: list[str] = []
    sample = path.read_text(encoding="utf-8", errors="replace")[:8192]
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
        score = avg - (var**0.5)
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

    with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
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


# ============================
# Cleaning steps (safe)
# ============================
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

    base = [snake_case(c) for c in df.columns]
    seen: dict[str, int] = {}
    uniq: list[str] = []
    for c in base:
        if c not in seen:
            seen[c] = 1
            uniq.append(c)
        else:
            seen[c] += 1
            uniq.append(f"{c}_{seen[c]}")

    if uniq != original:
        df.columns = uniq
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
    df, l = normalize_columns(df); changelog += l
    df, l = trim_whitespace_df(df); changelog += l
    df, l = remove_duplicates_and_empty_rows(df); changelog += l
    return df, changelog

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
# Near-duplicate analysis (preview/remove + examples)
# ============================
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
# Routes
# ============================
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
        rate_window=RATE_WINDOW_SECONDS,
        rate_max=RATE_MAX_UPLOADS,
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
    try:
        df = pd.read_csv(op, encoding="utf-8", sep=delim)
    except Exception as e:
        log_event("result_read_error", job_id=job_id, error=str(e))
        df = pd.DataFrame()

    repaired_indices = m.get("repaired_row_indices", []) or []
    preview_first, preview_last, preview_repaired = build_previews(df, repaired_indices)

    near_dupes_mode = (m.get("near_dupes_mode") or "").strip()
    ignored_cols = m.get("ignored_cols", []) or []
    examples_rows = m.get("near_dupe_examples_rows", []) or []
    near_dupe_examples_tables: List[str] = []
    if near_dupes_mode and examples_rows:
        near_dupe_examples_tables = [render_near_dupe_compare_table(ex, near_dupes_mode) for ex in examples_rows]

    payment_pending = bool(PAYMENTS_ENABLED and (not m.get("paid")) and m.get("stripe_session_id"))

    log_event(
        "result_view",
        job_id=job_id,
        paid=bool(m.get("paid")),
        payment_pending=payment_pending,
        rows=m.get("rows"),
        cols=m.get("cols"),
        delimiter=delim,
    )

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
        payment_pending=payment_pending,
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
            rate_window=RATE_WINDOW_SECONDS,
            rate_max=RATE_MAX_UPLOADS,
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
            rate_window=RATE_WINDOW_SECONDS,
            rate_max=RATE_MAX_UPLOADS,
        ), 429

    f = request.files.get("file")
    if not f or not f.filename:
        log_event("upload_missing_file")
        abort(400)

    original_filename = f.filename

    # Extension hint (server-side)
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
            rate_window=RATE_WINDOW_SECONDS,
            rate_max=RATE_MAX_UPLOADS,
        ), 400

    near_preview = bool(request.form.get("near_dupes_preview"))
    near_remove = bool(request.form.get("near_dupes_remove"))
    if near_remove:
        near_preview = True

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
    )

    f.save(rp)

    if not looks_like_text_file(rp):
        log_event("upload_rejected_binary", job_id=job_id, filename=original_filename)
        rp.unlink(missing_ok=True)
        return render_template_string(
            INDEX_HTML,
            error="That file doesn't look like a text CSV/TSV (binary data detected).",
            max_mb=MAX_BYTES // (1024 * 1024),
            retention=RETENTION_MINUTES,
            payments_enabled=PAYMENTS_ENABLED,
            max_rows=MAX_ROWS,
            max_cols=MAX_COLS,
            rate_window=RATE_WINDOW_SECONDS,
            rate_max=RATE_MAX_UPLOADS,
        ), 400

    parse_path, structural_log = normalize_line_endings_to_lf(rp, np)

    delim, delim_log = detect_delimiter(parse_path)
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
            rate_window=RATE_WINDOW_SECONDS,
            rate_max=RATE_MAX_UPLOADS,
        ), 400

    log_event(
        "upload_parsed",
        job_id=job_id,
        delimiter=delim,
        import_warning=import_warning,
        repaired_rows=len(repaired_indices),
        cols=len(df.columns),
        rows=len(df),
    )

    df2, clean_log = clean_csv(df)
    changelog = structural_log + delim_log + import_log + clean_log

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

    log_event(
        "upload_complete",
        job_id=job_id,
        delimiter=delim,
        rows=rows,
        cols=cols,
        near_dupes_mode=near_dupes_mode,
        near_dupes_count=near_dupes_count,
        paid=False,
    )

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
    )


@app.get("/download_original/<job_id>")
def download_original(job_id: str):
    cleanup_old_files()
    m = read_manifest(job_id)
    if not m:
        log_event("download_original_not_found", job_id=job_id)
        abort(404)

    p = WORK_DIR / str(m.get("original_file") or "")
    if not p.exists():
        log_event("download_original_missing_file", job_id=job_id)
        abort(404)

    log_event("download_original_served", job_id=job_id, filename=m.get("original_filename"))
    return send_file(p, as_attachment=True, download_name=m.get("original_filename") or "original.csv")


@app.get("/download/<job_id>")
def download(job_id: str):
    cleanup_old_files()
    m = read_manifest(job_id)
    op = out_path(job_id)
    if not m or not op.exists():
        log_event("download_not_found", job_id=job_id)
        abort(404)

    delim = m.get("detected_delimiter") or ","

    if not PAYMENTS_ENABLED:
        log_event("download_served_free", job_id=job_id, delimiter=delim)
        return send_file(op, as_attachment=True, download_name=cleaned_download_name(delim))

    if not m.get("paid") and m.get("stripe_session_id"):
        log_event("download_blocked_pending", job_id=job_id, stripe_session_id=m.get("stripe_session_id"))
        return redirect(f"/result/{job_id}", code=303)

    if not m.get("paid"):
        log_event("download_redirect_pay", job_id=job_id)
        return redirect(f"/pay/{job_id}", code=303)

    log_event("download_served_paid", job_id=job_id, delimiter=delim)
    return send_file(op, as_attachment=True, download_name=cleaned_download_name(delim))


@app.get("/pay/<job_id>")
def pay(job_id: str):
    cleanup_old_files()

    if not PAYMENTS_ENABLED:
        log_event("pay_disabled", job_id=job_id)
        return redirect(f"/download/{job_id}", code=303)

    m = read_manifest(job_id)
    op = out_path(job_id)
    if not m or not op.exists():
        log_event("pay_not_found", job_id=job_id)
        abort(404)

    session = stripe.checkout.Session.create(
        mode="payment",
        line_items=[{"price": PRICE_ID, "quantity": 1}],
        success_url=f"{BASE_URL}/success?job_id={job_id}&session_id={{CHECKOUT_SESSION_ID}}",
        cancel_url=f"{BASE_URL}/cancel?job_id={job_id}",
        metadata={"job_id": job_id},
    )

    m["stripe_session_id"] = session.get("id")
    manifest_path(job_id).write_text(json.dumps(m, indent=2), encoding="utf-8")

    log_event("pay_session_created", job_id=job_id, stripe_session_id=session.get("id"))
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
        log_event("success_confirmed_paid", job_id=job_id, stripe_session_id=session_id)
        return render_template_string(SUCCESS_HTML, job_id=job_id)

    log_event("success_not_confirmed", job_id=job_id, stripe_session_id=session_id, payment_status=getattr(sess, "payment_status", None))
    return "Payment not confirmed.", 402


@app.get("/cancel")
def cancel():
    job_id = (request.args.get("job_id") or "").strip()
    log_event("payment_canceled", job_id=job_id)
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