# -*- coding: utf-8 -*-
"""
server.py — Render-ready Flask app
- AUTH με X-API-Key (ENV: API_KEY)
- CORS enabled
- Ημερήσιο Excel αρχείο ΜΟΝΟ με στήλες: voucher, box, site
- Snapshots (FIFO) + endpoints: /flush, /list_snapshots, /get_file, /delete_file
- Συμβατότητα με /api/receipts και /api/receipts/upload
"""

from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import pandas as pd
import os, threading
from datetime import datetime
from typing import Optional

app = Flask(__name__)
CORS(app)

# ---------------------------- ΡΥΘΜΙΣΕΙΣ ----------------------------
API_KEY = os.environ.get("API_KEY", "")

def authed(req) -> bool:
    return bool(API_KEY) and req.headers.get("X-API-Key") == API_KEY

# Φάκελοι αποθήκευσης
PERMANENT_FOLDER = "/tmp/saved_files"                       # ημερήσιο "ζωντανό" αρχείο
SNAPSHOT_FOLDER  = os.path.join(PERMANENT_FOLDER, "snapshots")   # FIFO snapshots
UPLOADS_FOLDER   = os.path.join(PERMANENT_FOLDER, "uploads")     # (προαιρετικά) binary uploads

os.makedirs(PERMANENT_FOLDER, exist_ok=True)
os.makedirs(SNAPSHOT_FOLDER,  exist_ok=True)
os.makedirs(UPLOADS_FOLDER,   exist_ok=True)

# Συγχρονισμός προσπελάσεων αρχείων
FILE_LOCK = threading.Lock()

# ΜΟΝΟ αυτές οι στήλες κρατάμε στο Excel
ALLOWED_COLS = ["voucher", "box", "site"]

# ---------------------------- HELPERS ----------------------------
def get_daily_filename() -> str:
    return f"delivery_requests_{datetime.now().strftime('%Y%m%d')}.xlsx"

def get_daily_path() -> str:
    return os.path.join(PERMANENT_FOLDER, get_daily_filename())

def unique_snapshot_name() -> str:
    # microseconds για μοναδικότητα
    return f"delivery_requests_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.xlsx"

def _read_existing_excel(path: str) -> Optional[pd.DataFrame]:
    """Διάβασε υπάρχον .xlsx ως strings. Αν λείπει openpyxl/αποτύχει, γύρνα None."""
    if not os.path.exists(path):
        return None
    try:
        # Για .xlsx, το pandas χρησιμοποιεί openpyxl
        df = pd.read_excel(path, dtype=str)
        return df
    except Exception:
        return None

def ensure_df_and_append(row_dict: dict) -> tuple[str, int]:
    """
    Γράφει ΜΟΝΟ τις στήλες voucher, box, site στο ημερήσιο Excel.
    Η voucher μένει σε text format (στήλη A).
    Επιστρέφει (daily_filename, row_count).
    """
    daily_name = get_daily_filename()
    daily_path = get_daily_path()

    # Περιορίζουμε στα ALLOWED_COLS
    filtered = {k: ("" if row_dict.get(k) is None else str(row_dict.get(k))) for k in ALLOWED_COLS}
    df_new = pd.DataFrame([filtered], columns=ALLOWED_COLS)

    with FILE_LOCK:
        df_existing = _read_existing_excel(daily_path)
        if df_existing is not None:
            # Διατηρούμε ΜΟΝΟ τις ALLOWED_COLS
            df_existing = df_existing.reindex(columns=ALLOWED_COLS, fill_value="")
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            df_combined = df_new

        # Επιβάλλουμε string στο voucher (ασφάλεια)
        if "voucher" in df_combined.columns:
            df_combined["voucher"] = df_combined["voucher"].astype(str)

        # Γράψιμο με xlsxwriter και text format στη στήλη A (voucher)
        with pd.ExcelWriter(daily_path, engine='xlsxwriter') as writer:
            df_combined.to_excel(writer, index=False)
            ws = writer.sheets['Sheet1']
            text_fmt = writer.book.add_format({'num_format': '@'})
            ws.set_column('A:A', None, text_fmt)  # voucher ως text

    return daily_name, len(df_combined)

def fifo_oldest_snapshot() -> Optional[str]:
    snaps = [f for f in os.listdir(SNAPSHOT_FOLDER) if f.lower().endswith(".xlsx")]
    if not snaps:
        return None
    snaps.sort(key=lambda fn: os.path.getmtime(os.path.join(SNAPSHOT_FOLDER, fn)))
    return snaps[0]

# ---------------------------- ENDPOINTS ----------------------------

@app.get("/health")
def health():
    return jsonify(status="ok", time=datetime.utcnow().isoformat())

@app.get("/")
def home():
    return "✅ Botpress → Delivered API is running!"

# --------- ΠΑΡΑΓΩΓΙΚΗ ΡΟΗ (Ημερήσιο Excel + Snapshots) ---------

@app.post("/submit")
def submit():
    """Προσθήκη γραμμής (voucher, box, site) στο ημερήσιο Excel. (AUTH)"""
    if not authed(request):
        return jsonify(error="unauthorized"), 401

    data = request.get_json(silent=True) or {}
    voucher = data.get("voucher") or data.get("receipt_code")  # δέχεται και τα δύο
    box     = data.get("box", "")
    site    = data.get("site", "")

    if not voucher:
        return jsonify(success=False, error="Missing voucher/receipt_code"), 400

    daily_name, row_count = ensure_df_and_append({
        "voucher": str(voucher),
        "box": box,
        "site": site,
    })

    return jsonify(success=True, file=daily_name, row_count=row_count)

@app.get("/flush")
def flush():
    """
    FIFO snapshots:
      1) Αν υπάρχουν snapshots -> δώσε το παλαιότερο.
      2) Αλλιώς, αν υπάρχει ημερήσιο -> rotate σε νέο snapshot και δώστο.
      3) Αλλιώς 404.
    """
    if not authed(request):
        return jsonify(error="unauthorized"), 401

    daily_path = get_daily_path()

    with FILE_LOCK:
        # (1) Παλαιότερο snapshot;
        oldest = fifo_oldest_snapshot()
        if oldest:
            snap_path = os.path.join(SNAPSHOT_FOLDER, oldest)
            try:
                return send_file(snap_path, as_attachment=True, download_name=oldest)
            except TypeError:
                return send_file(snap_path, as_attachment=True, attachment_filename=oldest)

        # (2) Rotate daily -> snapshot
        if os.path.exists(daily_path):
            snap_name = unique_snapshot_name()
            snap_path = os.path.join(SNAPSHOT_FOLDER, snap_name)
            os.replace(daily_path, snap_path)  # atomic move
            try:
                return send_file(snap_path, as_attachment=True, download_name=snap_name)
            except TypeError:
                return send_file(snap_path, as_attachment=True, attachment_filename=snap_name)

    # (3) Τίποτα διαθέσιμο
    return jsonify(success=False, error="No file to download"), 404

@app.get("/list_snapshots")
def list_snapshots():
    """Λίστα διαθέσιμων snapshots (FIFO σειρά)."""
    if not authed(request):
        return jsonify(error="unauthorized"), 401

    snaps = [f for f in os.listdir(SNAPSHOT_FOLDER) if f.lower().endswith(".xlsx")]
    snaps.sort(key=lambda fn: os.path.getmtime(os.path.join(SNAPSHOT_FOLDER, fn)))
    return jsonify(count=len(snaps), files=snaps)

@app.get("/get_file")
def get_file():
    """Κατέβασε συγκεκριμένο snapshot ή το ημερήσιο αρχείο. (AUTH)"""
    if not authed(request):
        return jsonify(error="unauthorized"), 401

    filename = request.args.get('filename')
    if not filename:
        return jsonify(success=False, error="Filename required"), 400

    path_snap  = os.path.join(SNAPSHOT_FOLDER, filename)
    path_daily = os.path.join(PERMANENT_FOLDER, filename)

    if os.path.exists(path_snap):
        return send_file(path_snap, as_attachment=True)
    if os.path.exists(path_daily):
        return send_file(path_daily, as_attachment=True)
    return jsonify(success=False, error="File not found"), 404

@app.get("/delete_file")
def delete_file():
    """ΔΙΑΓΡΑΦΕΙ ΜΟΝΟ snapshots (όχι το ζωντανό ημερήσιο). (AUTH)"""
    if not authed(request):
        return jsonify(error="unauthorized"), 401

    filename = request.args.get('filename')
    if not filename:
        return jsonify(success=False, error="Filename required"), 400

    path_snap  = os.path.join(SNAPSHOT_FOLDER, filename)
    path_daily = os.path.join(PERMANENT_FOLDER, filename)

    if os.path.exists(path_snap):
        os.remove(path_snap)
        return jsonify(success=True, message=f"{filename} deleted")
    if os.path.exists(path_daily):
        return jsonify(success=False, error="Refuse to delete daily file"), 403
    return jsonify(success=False, error="File not found"), 404

# --------- Συμβατότητα με το παλιό API (γράφει ΜΟΝΟ voucher, box, site) ---------

@app.post("/api/receipts")
def receipts_json():
    """
    JSON input: { "receipt_code": "...", "voucher": "...", "site": "...", "box": "..." }
    - Δεκτό είτε receipt_code είτε voucher (το πρώτο που υπάρχει).
    - Γράφει μόνο voucher/box/site στο Excel.
    """
    if not authed(request):
        return jsonify(error="unauthorized"), 401

    data = request.get_json(silent=True) or {}
    code = data.get("receipt_code") or data.get("voucher")
    if not code:
        return jsonify(error="missing receipt_code/voucher"), 400

    site = data.get("site", "")
    box  = data.get("box", "")

    daily_name, row_count = ensure_df_and_append({
        "voucher": str(code),
        "box": box,
        "site": site,
    })

    return jsonify(ok=True, received={
        "voucher": str(code),
        "box": box,
        "site": site,
        "daily_file": daily_name,
        "row_count": row_count
    })

@app.post("/api/receipts/upload")
def receipts_upload():
    """
    form-data input: receipt_code or voucher, site, box, (προαιρετικά file)
    - Ανέβασμα αρχείου είναι προαιρετικό και ΔΕΝ αποθηκεύεται στο Excel.
    - Στο Excel γράφουμε μόνο voucher/box/site.
    """
    if not authed(request):
        return jsonify(error="unauthorized"), 401

    code = request.form.get("receipt_code") or request.form.get("voucher")
    if not code:
        return jsonify(error="missing receipt_code/voucher"), 400

    site = request.form.get("site", "")
    box  = request.form.get("box", "")

    # Αν θες να αποθηκεύεις και το binary σε φάκελο, μπορείς εδώ:
    # f = request.files.get("file")
    # if f and f.filename:
    #     safe_name = f"{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}_{f.filename}"
    #     f.save(os.path.join(UPLOADS_FOLDER, safe_name))

    daily_name, row_count = ensure_df_and_append({
        "voucher": str(code),
        "box": box,
        "site": site,
    })

    return jsonify(ok=True, received={
        "voucher": str(code),
        "box": box,
        "site": site,
        "daily_file": daily_name,
        "row_count": row_count
    })

# ---------------------------- MAIN ----------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
