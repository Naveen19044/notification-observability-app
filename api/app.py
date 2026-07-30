import os
import json
import time
import logging
import uuid
from datetime import datetime

from flask import Flask, request, jsonify
from flask_cors import CORS
import redis
import psycopg2
from psycopg2.extras import RealDictCursor
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST

app = Flask(__name__)
CORS(app)  # allow the UI (served from a different port) to call this API

# ---------- structured JSON logging ----------
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_obj = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "service": "notification-api",
            "message": record.getMessage(),
        }
        return json.dumps(log_obj)

logger = logging.getLogger("notification-api")
handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# ---------- connections ----------
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
DB_HOST = os.environ.get("DB_HOST", "postgres")
DB_NAME = os.environ.get("DB_NAME", "notifdb")
DB_USER = os.environ.get("DB_USER", "notifuser")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "notifpass")

r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, cursor_factory=RealDictCursor
    )

def init_db():
    for attempt in range(10):
        try:
            conn = get_db_conn()
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS notifications (
                    id UUID PRIMARY KEY,
                    to_address TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    body TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                );
            """)
            conn.commit()
            cur.close()
            conn.close()
            logger.info("Database initialized successfully")
            return
        except Exception as e:
            logger.info(f"Waiting for database... attempt {attempt+1}")
            time.sleep(3)
    raise Exception("Could not connect to database after multiple attempts")

# ---------- prometheus metrics ----------
REQUEST_COUNT = Counter("api_requests_total", "Total API requests", ["endpoint", "method", "status"])
REQUEST_LATENCY = Histogram("api_request_latency_seconds", "Request latency", ["endpoint"])
QUEUE_DEPTH = Gauge("notification_queue_depth", "Current number of jobs waiting in the Redis queue")

@app.before_request
def start_timer():
    request.start_time = time.time()

@app.after_request
def record_metrics(response):
    latency = time.time() - request.start_time
    REQUEST_LATENCY.labels(endpoint=request.path).observe(latency)
    REQUEST_COUNT.labels(endpoint=request.path, method=request.method, status=response.status_code).inc()
    return response

# ---------- routes ----------
@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "notification-api", "timestamp": datetime.utcnow().isoformat() + "Z"})

@app.route("/metrics", methods=["GET"])
def metrics():
    QUEUE_DEPTH.set(r.llen("notification_queue"))
    return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}

@app.route("/api/notifications", methods=["POST"])
def create_notification():
    data = request.get_json(force=True, silent=True) or {}
    to_address = data.get("to")
    subject = data.get("subject")
    body = data.get("body", "")

    if not to_address or not subject:
        logger.info(json.dumps({"event": "validation_failed", "data": data}))
        return jsonify({"error": "to and subject are required"}), 400

    # optional forced-failure test switch: POST /api/notifications?fail=true
    if request.args.get("fail") == "true":
        logger.info(json.dumps({"event": "simulated_rejection", "to": to_address}))
        return jsonify({"error": "Simulated rejection for testing"}), 500

    notif_id = str(uuid.uuid4())
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO notifications (id, to_address, subject, body, status) VALUES (%s, %s, %s, %s, 'pending')",
        (notif_id, to_address, subject, body),
    )
    conn.commit()
    cur.close()
    conn.close()

    job = {"id": notif_id, "to": to_address, "subject": subject, "body": body}
    r.rpush("notification_queue", json.dumps(job))

    logger.info(json.dumps({"event": "notification_queued", "id": notif_id, "to": to_address}))

    return jsonify({"id": notif_id, "status": "pending", "message": "Notification accepted for delivery"}), 202

@app.route("/api/notifications/<notif_id>", methods=["GET"])
def get_notification(notif_id):
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM notifications WHERE id = %s", (notif_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return jsonify({"error": "not found"}), 404
    return jsonify(dict(row))

@app.route("/api/notifications", methods=["GET"])
def list_notifications():
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM notifications ORDER BY created_at DESC LIMIT 50")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route("/api/queue-depth", methods=["GET"])
def queue_depth():
    return jsonify({"depth": r.llen("notification_queue")})

# ---------- chaos controls ----------
@app.route("/chaos/settings", methods=["GET"])
def get_chaos_settings():
    return jsonify({
        "fail_rate": float(r.get("chaos:fail_rate") or 0),
        "slow_mode": r.get("chaos:slow_mode") == "1",
        "worker_paused": r.get("chaos:worker_paused") == "1",
    })

@app.route("/chaos/fail-rate", methods=["POST"])
def set_fail_rate():
    data = request.get_json(force=True, silent=True) or {}
    rate = float(data.get("rate", 0))
    r.set("chaos:fail_rate", rate)
    logger.info(json.dumps({"event": "chaos_fail_rate_changed", "rate": rate}))
    return jsonify({"fail_rate": rate})

@app.route("/chaos/slow-mode", methods=["POST"])
def set_slow_mode():
    data = request.get_json(force=True, silent=True) or {}
    enabled = bool(data.get("enabled", False))
    r.set("chaos:slow_mode", "1" if enabled else "0")
    logger.info(json.dumps({"event": "chaos_slow_mode_changed", "enabled": enabled}))
    return jsonify({"slow_mode": enabled})

@app.route("/chaos/worker-pause", methods=["POST"])
def set_worker_pause():
    data = request.get_json(force=True, silent=True) or {}
    paused = bool(data.get("paused", False))
    r.set("chaos:worker_paused", "1" if paused else "0")
    logger.info(json.dumps({"event": "chaos_worker_pause_changed", "paused": paused}))
    return jsonify({"worker_paused": paused})

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5001)
