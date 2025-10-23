# -*- coding: utf-8 -*-
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import pandas as pd
import os, threading
from datetime import datetime
from werkzeug.utils import secure_filename

app = Flask(__name__)
CORS(app)

# ---------------------------- ΡΥΘΜΙΣΕΙΣ ----------------------------
# API Key auth (κρατάμε τη συμπεριφορά του 1ου app)
API_KEY = os.environ.get("API_KEY", "")

def authed(req):
    return bool(API_KEY) and req.headers.get("X-API-Key") == API_KEY

# Φάκελοι αποθήκευσης (όπως στο 2ο app, + uploads για binary αρχεία)
PERMANENT_FOLDER = "/tmp/saved_files"                     # ημερήσιο "ζωντανό" αρχείο
SNAPSHOT_FOLDER  = os.path.join(PERMANENT_FOLDER, "snapshots")   # μοναδικά snapshots (FIFO)
UPLOADS_FOLDER   = os.path.join(PERMANENT_FOLDER, "uploads")     # αποθήκευση uploaded files (προαιρετικό)

os.makedirs(PERMANENT_FOLDER, exist_ok=True)
os.makedirs(SNAPSHOT_FOLDER,  exist_ok=True)
os.makedirs(UPLOADS_FOLDER,   exist_ok=True)

# Συγχρονισμός προσπελάσεων αρχείων
FILE_LOCK = threading.Lock()

# ---------------------------- HELPERS ----------------------------
def get_daily_filename() -> str:
    return f"delivery_requests_{datetime.now().strftime('%Y%m%d')}.xlsx"

def get_daily_path() -> str:
    return os.path.join(PERMANENT_FOLDER, get_daily_filename())

def unique_snapshot_name() -> str:
    # microseconds για 100% μοναδικότητα
    return f"delivery_requests_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.xlsx"

def ensure_df_and_append(row_dict: dict) -> (str, int):
    """
    Φορτώνει/δημιουργεί το ημερήσιο Excel, προσθέτει τη γραμμή, επιβάλλει text στο voucher,
    και επιστρέφει (filename, row_count).
    """
    daily_name = get_daily_filename()
    daily_path = get_daily_path()

    # Μετατρέπουμε σε DataFrame
    df_new = pd.DataFrame([row_dict])

    with FILE_LOCK:
        if os.path.exists(daily_path):
            df_existing = pd.read_excel(daily_path, converters={"voucher": str})
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            df_combined = df_new

        # Επιβάλλουμε string στο voucher
        if "voucher" in df_combined.columns:
            df_combined["voucher"] = df_combined["voucher"].astype(str)

        # Γράψιμο με xlsxwriter και text format στη στήλη A (voucher)
        with pd.ExcelWriter(daily_path, engine='xlsxwriter') as writer:
            df_combined.to_excel(writer, index=False)
            ws = writer.sheets['Sheet1']
            # Force text στο voucher (στήλη A)
            text_fmt = writer.book.add_format({'num_format': '@'})
            ws.set_column('A:A', None, text_fmt)

    return daily_name, len(df_combined)

def fifo_oldest_snapshot() -> str | None:
    snaps = sorted(
        [f for f in os.listdir(SNAPSHOT_FOLDER) if f.lower().endswith(".xlsx")],
        key=lambda fn: os.path.getmtime(os.path.join(SNAPSHOT_FOLDER, fn))
    )
    return snaps[0] if snaps else None

def save_uploaded_file(file_storage, prefix: str = "") -> tuple[str, int] | None:
    """
    Αποθηκεύει το ανεβασμένο αρχείο στον UPLOADS_FOLDER.
    Επιστρέφει (relative_filename, size_bytes) ή None αν δεν υπήρχε αρχείο.
    """
    if not file_storage:
        return None
    filename = secure_filename(file_storage.filename) or "upload.bin"
    tstamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    final_name = f"{prefix}_{tstamp}_{filename}" if prefix else f"{tstamp}_{filename}"
    final_path = os.path.join(UPLOADS_FOLDER, final_name)
    file_storage.save(final_path)
    size = os.path.getsize(final_path)
    return final_name, size

# ---------------------------- ENDPOINTS ----------------------------

# Health (ίδιο semantics με το 1ο)
@app.get("/health")
def health():
    return jsonify(status="ok", time=datetime.utcnow().isoformat())

# Αρχική (όπως στο 2ο)
@app.get("/")
def home():
    return "✅ Botpress → Delivered API is running!"

# ---------- ΝΕΑ ΡΟΗ ΠΑΡΑΓΩΓΗΣ ΑΡΧΕΙΩΝ (από το 2ο app) ----------

@app.post("/submit")
def submit():
    """Προσθήκη εγγραφής στο ημερήσιο αρχείο (AUTH required)."""
    if not authed(request):
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(silent=True)
    required = ['voucher', 'delivery_station', 'delivery_date_with_weekday', 'more_instructions']
    if not data or not all(k in data for k in required):
        return jsonify({"success": False, "error": "Missing fields"}), 400

    daily_name, row_count = ensure_df_and_append({
        "voucher":                     str(data.get("voucher", "")),
        "delivery_station":            data.get("delivery_station", ""),
        "delivery_date_with_weekday":  data.get("delivery_date_with_weekday", ""),
        "more_instructions":           data.get("more_instructions", ""),
        # προαιρετικά meta:
        "created_at":                  datetime.utcnow().isoformat() + "Z",
        "source":                      "submit"
    })

    return jsonify({
        "success": True,
        "message": "Appended to daily file",
        "file": daily_name,
        "row_count": row_count
    })

@app.get("/flush")
def flush():
    """
    FIFO snapshots:
      1) Αν ΥΠΑΡΧΟΥΝ snapshots: επιστρέφει το ΠΑΛΑΙΟΤΕΡΟ (για καθάρισμα backlog).
      2) Αλλιώς, αν υπάρχει daily: κάνει rotate σε ΝΕΟ μοναδικό snapshot και το επιστρέφει.
      3) Αλλιώς 404.
    """
    if not authed(request):
        return jsonify({"error": "unauthorized"}), 401

    daily_path = get_daily_path()

    with FILE_LOCK:
        # (1) Παλαιότερο snapshot αν υπάρχει
        oldest = fifo_oldest_snapshot()
        if oldest:
            snap_path = os.path.join(SNAPSHOT_FOLDER, oldest)
            try:
                return send_file(snap_path, as_attachment=True, download_name=oldest)
            except TypeError:
                # Flask < 2.0
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
    return jsonify({"success": False, "error": "No file to download"}), 404

@app.get("/get_file")
def get_file():
    """Download snapshot ή ημερήσιο αρχείο (debug/έλεγχος)."""
    if not authed(request):
        return jsonify({"error": "unauthorized"}), 401

    filename = request.args.get('filename')
    if not filename:
        return jsonify({"success": False, "error": "Filename required"}), 400

    path_snap  = os.path.join(SNAPSHOT_FOLDER, filename)
    path_daily = os.path.join(PERMANENT_FOLDER, filename)
    if os.path.exists(path_snap):
        return send_file(path_snap, as_attachment=True)
    if os.path.exists(path_daily):
        return send_file(path_daily, as_attachment=True)
    return jsonify({"success": False, "error": "File not found"}), 404

@app.get("/delete_file")
def delete_file():
    """ΔΙΑΓΡΑΦΕΙ ΜΟΝΟ snapshots (όχι το ζωντανό ημερήσιο)."""
    if not authed(request):
        return jsonify({"error": "unauthorized"}), 401

    filename = request.args.get('filename')
    if not filename:
        return jsonify({"success": False, "error": "Filename required"}), 400

    path_snap  = os.path.join(SNAPSHOT_FOLDER, filename)
    path_daily = os.path.join(PERMANENT_FOLDER, filename)

    if os.path.exists(path_snap):
        os.remove(path_snap)
        return jsonify({"success": True, "message": f"{filename} deleted"})
    if os.path.exists(path_daily):
        return jsonify({"success": False, "error": "Refuse to delete daily file"}), 403
    return jsonify({"success": False, "error": "File not found"}), 404

@app.get("/list_snapshots")
def list_snapshots():
    """Λίστα διαθέσιμων snapshots (FIFO σειρά)."""
    if not authed(request):
        return jsonify({"error": "unauthorized"}), 401

    snaps = sorted(
        [f for f in os.listdir(SNAPSHOT_FOLDER) if f.lower().endswith(".xlsx")],
        key=lambda fn: os.path.getmtime(os.path.join(SNAPSHOT_FOLDER, fn))
    )
    return jsonify({"count": len(snaps), "files": snaps})

# ---------- ΣΥΜΒΑΤΟΤΗΤΑ ΜΕ ΤΟ 1ο APP ----------

@app.post("/api/receipts")
def receipts_json():
    """
    Συμβατό με το παλιό endpoint:
    - Κάνει AUTH
    - Παίρνει JSON {timestamp, receipt_code, ...}
    - Αντιστοιχεί στα fields του /submit και προσθέτει γραμμή στο ημερήσιο Excel.
    """
    if not authed(request):
        return jsonify(error="unauthorized"), 401

    data = request.get_json(silent=True) or {}
    ts   = data.get("timestamp")
    code = data.get("receipt_code")
    # Επιτρέπουμε ελάχιστα απαιτούμενα (voucher + delivery_date_with_weekday)
    if not ts or not code:
        return jsonify(error="missing timestamp/receipt_code"), 400

    # Mapping -> νέα ροή
    mapped = {
        "voucher": str(code),
        "delivery_station": data.get("delivery_station", ""),  # αν δεν δοθεί, μένει κενό
        "delivery_date_with_weekday": ts,
        "more_instructions": data.get("more_instructions", "")
    }

    daily_name, row_count = ensure_df_and_append({
        **mapped,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "source":     "api/receipts"
    })

    app.logger.info(f"[JSON] {ts} | {code} -> appended to {daily_name}")
    return jsonify(ok=True, received={"timestamp": ts, "receipt_code": code, "daily_file": daily_name, "row_count": row_count})

@app.post("/api/receipts/upload")
def receipts_upload():
    """
    Συμβατό με το παλιό endpoint:
    - Κάνει AUTH
    - Δέχεται form-data: timestamp, receipt_code, file (προαιρετικό)
    - Αποθηκεύει το αρχείο (αν υπάρχει) στον UPLOADS_FOLDER
    - Γράφει και μια γραμμή στο ημερήσιο Excel με meta (file_bytes, upload_filename)
    """
    if not authed(request):
        return jsonify(error="unauthorized"), 401

    ts   = request.form.get("timestamp")
    code = request.form.get("receipt_code")
    f    = request.files.get("file")

    if not ts or not code:
        return jsonify(error="missing timestamp/receipt_code"), 400

    saved = save_uploaded_file(f, prefix=str(code)) if f else None
    upload_filename, size = (saved[0], saved[1]) if saved else ("", 0)

    daily_name, row_count = ensure_df_and_append({
        "voucher": str(code),
        "delivery_station": request.form.get("delivery_station", ""),
        "delivery_date_with_weekday": ts,
        "more_instructions": request.form.get("more_instructions", ""),
        "file_bytes": size,
        "upload_filename": upload_filename,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "source": "api/receipts/upload"
    })

    app.logger.info(f"[UPLOAD] {ts} | {code} | file_bytes={size} | file={upload_filename} -> appended to {daily_name}")
    return jsonify(ok=True, received={
        "timestamp": ts,
        "receipt_code": code,
        "file_bytes": size,
        "upload_filename": upload_filename,
        "daily_file": daily_name,
        "row_count": row_count
    })

# ---------------------------- MAIN ----------------------------
if __name__ == "__main__":
    # Επιτρέπει override port από ENV (π.χ. Render/Cloud)
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
