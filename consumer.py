import base64
import json
import logging
import os
import cv2
import numpy as np
import psycopg2
from dotenv import load_dotenv
from kafka import KafkaConsumer
from ultralytics import YOLO

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

load_dotenv()
db_password = os.getenv("DB_PASSWORD")

logging.info("Booting up AI Worker Node...")
model = YOLO("yolov8n.pt")

conn = None
consumer = None

try:
    # -----------------------------------------------------------------------
    # Database
    # -----------------------------------------------------------------------
    logging.info("Connecting to PostgreSQL...")
    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        user="admin",
        password=db_password,
        dbname="vision_db",
    )
    conn.autocommit = True
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS traffic_logs (
            id            SERIAL PRIMARY KEY,
            camera_id     VARCHAR(50),
            timestamp     FLOAT,
            person_count  INT,
            car_count     INT
        );
    """)
    logging.info("Database ready.")

    # -----------------------------------------------------------------------
    # Kafka consumer
    # FIX: Changed auto_offset_reset from "latest" to "earliest".
    #      "latest" means if this worker restarts it silently skips every
    #      frame that arrived while it was down. "earliest" replays from the
    #      last committed offset so no frames are lost.
    # -----------------------------------------------------------------------
    consumer = KafkaConsumer(
        "video-stream",
        bootstrap_servers="kafka:9092",
        auto_offset_reset="earliest",       # FIX: was "latest"
        enable_auto_commit=True,
        group_id="ai-worker-group",         # FIX: added group_id so Kafka tracks
    )                                       #      the committed offset per consumer group
    logging.info("Connected to Kafka. Waiting for frames...")

    # -----------------------------------------------------------------------
    # Main processing loop
    # -----------------------------------------------------------------------
    for message in consumer:
        # --- Deserialise ---------------------------------------------------
        try:
            data = json.loads(message.value.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logging.warning(f"Skipping malformed Kafka message: {e}")
            continue

        cam_id    = data.get("camera_id", "UNKNOWN")
        timestamp = data.get("timestamp", 0.0)

        # --- Decode frame --------------------------------------------------
        try:
            img_bytes = base64.b64decode(data["image_data"])
            nparr     = np.frombuffer(img_bytes, np.uint8)
            frame     = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        except Exception as e:
            logging.warning(f"[{cam_id}] Failed to decode image bytes: {e}. Skipping frame.")
            continue

        # FIX: imdecode returns None when the buffer is corrupt/truncated.
        #      Without this guard, model(None) raises an unhandled exception
        #      that crashes the entire worker.
        if frame is None:
            logging.warning(f"[{cam_id}] cv2.imdecode returned None (corrupt frame). Skipping.")
            continue

        # --- AI inference --------------------------------------------------
        try:
            results = model(frame, stream=True, verbose=False)
        except Exception as e:
            logging.error(f"[{cam_id}] YOLO inference failed: {e}. Skipping frame.")
            continue

        person_count = 0
        car_count    = 0

        for r in results:
            classes       = r.boxes.cls.tolist()
            person_count += classes.count(0)   # COCO class 0 = person
            car_count    += classes.count(2)   # COCO class 2 = car

        # --- Persist -------------------------------------------------------
        cursor.execute(
            """
            INSERT INTO traffic_logs (camera_id, timestamp, person_count, car_count)
            VALUES (%s, %s, %s, %s)
            """,
            (cam_id, timestamp, person_count, car_count),
        )

        # FIX: Duplicate logging.info line removed (same line appeared twice).
        logging.info(
            f"Saved  →  Camera: {cam_id} | Persons: {person_count} | Cars: {car_count}"
        )

except KeyboardInterrupt:
    logging.warning("Shutdown signal received (Ctrl+C). Halting worker...")
except Exception as e:
    logging.error(f"Worker crashed due to an unexpected error: {e}", exc_info=True)
finally:
    logging.info("Executing cleanup procedures...")
    if consumer is not None:
        consumer.close()
        logging.info("Kafka consumer closed.")
    if conn is not None:
        conn.close()
        logging.info("PostgreSQL connection closed.")
    logging.info("Worker safely terminated.")
