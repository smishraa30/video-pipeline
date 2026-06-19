import base64
import json
import logging
import time
import threading
import cv2
from fastapi import FastAPI, BackgroundTasks
from kafka import KafkaProducer
from psycopg2 import pool
import os
from dotenv import load_dotenv

# Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

load_dotenv()
db_password = os.getenv("DB_PASSWORD")

app = FastAPI()

# FIX: Use a threading.Event for safe cross-thread signalling instead of a
#      bare global bool.
_stop_event = threading.Event()

producer = KafkaProducer(bootstrap_servers="kafka:9092")
TOPIC_NAME = "video-stream"

#      every analytics request.
db_pool = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=5,
    host="postgres",   # FIX: was "localhost" — inside Docker use the service name
    port=5432,
    user="admin",
    password=db_password,
    dbname="vision_db",
)


def capture_and_send_frames(source: str, camera_id: str) -> None:
    """Capture frames from *source*, sample every 5th one, and publish to Kafka."""

    _stop_event.clear()

    video_source: int | str = int(source) if source.isdigit() else source
    label = f"webcam (device {video_source})" if isinstance(video_source, int) else video_source
    logging.info(f"[PRODUCER] Opening source: {label}")

    cap = cv2.VideoCapture(video_source)
    if not cap.isOpened():
        logging.error(f"[PRODUCER] Could not open video source: {source}")
        return

    frame_counter = 0

    try:
        while not _stop_event.is_set():
            success, frame = cap.read()

            if not success:
                if isinstance(video_source, str):
                    # Loop the file back to the beginning
                    cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
                    continue
                else:
                    logging.warning("[PRODUCER] Webcam feed interrupted.")
                    break

            frame_counter += 1

            if frame_counter % 5 != 0:
                continue

            frame = cv2.resize(frame, (640, 480))
            _, buffer = cv2.imencode(".jpg", frame)
            jpg_as_text = base64.b64encode(buffer).decode("utf-8")

            payload = {
                "camera_id": camera_id,
                "timestamp": time.time(),
                "image_data": jpg_as_text,
            }

            producer.send(TOPIC_NAME, json.dumps(payload).encode("utf-8"))

    finally:
        cap.release()
        producer.flush()
        logging.info("[PRODUCER] Stream deactivated and Kafka buffer flushed.")

# API endpoints
@app.get("/health")
def health_check():
    return {"status": "OK"}


@app.post("/start")
def start_stream(
    background_tasks: BackgroundTasks,
    source: str = "test_video.mp4",
    camera_id: str = "CAM-01",
):
    if not _stop_event.is_set() and _stop_event.is_set() is False:
        # Check whether a stream is already running via the event flag.
        pass

    if _stop_event.is_set():
        return {"status": "ALREADY_RUNNING", "message": "A stream is already active. POST /stop first."}

    background_tasks.add_task(capture_and_send_frames, source, camera_id)
    logging.info(f"[API] Stream started — source={source}, camera_id={camera_id}")
    return {"status": "SUCCESS", "message": f"Streaming started from source: {source}"}


@app.post("/stop")
def stop_stream():
    _stop_event.set()
    logging.info("[API] Stop signal sent.")
    return {"status": "SUCCESS", "message": "Stop signal sent to streaming worker."}


@app.get("/analytics/stats")
def get_traffic_stats():
    # FIX: Was connecting to host="localhost" — must be "postgres" inside Docker.
    # FIX: Now borrows from the connection pool instead of creating a new
    #      connection on every request.
    conn = None
    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()
        cursor.execute("SELECT SUM(car_count), SUM(person_count) FROM traffic_logs;")
        result = cursor.fetchone()

        return {
            "status": "SUCCESS",
            "total_cars": result[0] or 0,
            "total_persons": result[1] or 0,
        }
    except Exception as e:
        logging.error(f"[DB] Analytics query failed: {e}")
        return {"status": "ERROR", "message": str(e)}
    finally:
        if conn:
            db_pool.putconn(conn)   # Return connection to pool, never leak it

# Graceful shutdown — close Kafka producer when Uvicorn exits
@app.on_event("shutdown")
def shutdown_event():
    _stop_event.set()
    producer.flush()
    producer.close()
    db_pool.closeall()
    logging.info("[SYSTEM] All connections closed. Goodbye.")
