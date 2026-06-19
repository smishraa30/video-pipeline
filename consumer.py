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

# 1. ENTERPRISE LOGGING SETUP
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

load_dotenv()
db_password = os.getenv("DB_PASSWORD")

logging.info("Booting up AI Worker Node...")
model = YOLO("yolov8n.pt") 

# Declare connections as None initially so the 'finally' block doesn't crash if they fail to open
conn = None
consumer = None

try:
    # --- DATABASE SETUP ---
    logging.info("Connecting to PostgreSQL...")
    conn = psycopg2.connect(
        host="localhost", port=5432, 
        user="admin", password=db_password, dbname="vision_db"
    )
    conn.autocommit = True
    cursor = conn.cursor()
    logging.info("Database Ready.")

    # --- KAFKA SETUP ---
    consumer = KafkaConsumer(
        'video-stream',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest'
    )
    logging.info("Connected to Kafka. Waiting for data...")

    # --- THE HEADLESS PROCESSING LOOP ---
    for message in consumer:
        data = json.loads(message.value.decode('utf-8'))
        cam_id = data["camera_id"]
        timestamp = data["timestamp"]
        
        img_bytes = base64.b64decode(data["image_data"])
        nparr = np.frombuffer(img_bytes, np.uint8)
        frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        
        results = model(frame, stream=True, verbose=False)
        
        person_count = 0
        car_count = 0
        
        for r in results:
            classes = r.boxes.cls.tolist()
            person_count += classes.count(0) 
            car_count += classes.count(2)    
            
        cursor.execute(
            "INSERT INTO traffic_logs (camera_id, timestamp, person_count, car_count) VALUES (%s, %s, %s, %s)",
            (cam_id, timestamp, person_count, car_count)
        )
        
        # Use logging.info instead of print!
        logging.info(f"Saved -> Camera: {cam_id} | Persons: {person_count} | Cars: {car_count}")

# 2. GRACEFUL SHUTDOWN HANDLING
except KeyboardInterrupt:
    logging.warning("Shutdown signal received (Ctrl+C). Halting worker...")
except Exception as e:
    logging.error(f"Worker crashed due to an unexpected error: {e}")
finally:
    logging.info("Executing cleanup procedures...")
    if consumer is not None:
        consumer.close()
        logging.info("Kafka connection closed.")
    if conn is not None:
        conn.close()
        logging.info("PostgreSQL connection closed.")
    logging.info("Worker safely terminated.")