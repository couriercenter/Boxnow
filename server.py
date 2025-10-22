from flask import Flask, request, jsonify
import os
from datetime import datetime

app = Flask(__name__)
API_KEY = os.environ.get("API_KEY", "")

def authed(req):
    return API_KEY and req.headers.get("X-API-Key") == API_KEY

@app.get("/health")
def health():
    return jsonify(status="ok", time=datetime.utcnow().isoformat())

@app.post("/api/receipts")
def receipts_json():
    if not authed(request):
        return jsonify(error="unauthorized"), 401
    data = request.get_json(silent=True) or {}
    ts = data.get("timestamp")
    code = data.get("receipt_code")
    if not ts or not code:
        return jsonify(error="missing timestamp/receipt_code"), 400
    app.logger.info(f"[JSON] {ts} | {code}")
    return jsonify(ok=True, received={"timestamp": ts, "receipt_code": code})

@app.post("/api/receipts/upload")
def receipts_upload():
    if not authed(request):
        return jsonify(error="unauthorized"), 401
    ts = request.form.get("timestamp")
    code = request.form.get("receipt_code")
    f = request.files.get("file")
    if not ts or not code:
        return jsonify(error="missing timestamp/receipt_code"), 400
    size = len(f.read()) if f else 0
    app.logger.info(f"[UPLOAD] {ts} | {code} | file_bytes={size}")
    return jsonify(ok=True, received={"timestamp": ts, "receipt_code": code, "file_bytes": size})
