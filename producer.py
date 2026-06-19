import base64
import json
import time
import cv2
from fastapi import FastAPI, BackgroundTasks
from kafka import KafkaProducer
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
db_password = os.getenv("DB_PASSWORD")
app = FastAPI()

producer = KafkaProducer(bootstrap_servers='localhost:9092')
TOPIC_NAME = 'video-stream'
is_streaming = False

def capture_and_send_frames():
    global is_streaming
    video_path = "test_video.mp4" 
    cap = cv2.VideoCapture(video_path) 
    
    print(f"[SYSTEM] Video file loaded: {video_path}. Sending structured data to Kafka...")
    
    frame_counter = 0 
    
    while is_streaming:
        success, frame = cap.read()
        if not success:
            cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
            continue
            
        frame_counter += 1
        
        if frame_counter % 5 != 0:
            time.sleep(0.03) 
            continue
        
        frame = cv2.resize(frame, (640, 480))
        _, buffer = cv2.imencode('.jpg', frame)
        
        jpg_as_text = base64.b64encode(buffer).decode('utf-8')
        
        payload = {
            "camera_id": "CAM-01",
            "timestamp": time.time(),
            "image_data": jpg_as_text
        }
        
        producer.send(TOPIC_NAME, json.dumps(payload).encode('utf-8'))
        time.sleep(0.03)

    cap.release()
    print("[SYSTEM] Stream deactivated.")

@app.post("/start")
def start_stream(background_tasks: BackgroundTasks):
    global is_streaming
    if not is_streaming:
        is_streaming = True
        background_tasks.add_task(capture_and_send_frames)
    return {"status": "SUCCESS", "message": "Camera started and streaming to Kafka."}

@app.post("/stop")
def stop_stream():
    global is_streaming
    is_streaming = False
    return {"status": "SUCCESS", "message": "Camera stopped."}

@app.get("/analytics/stats")
def get_traffic_stats():
    try:
        conn = psycopg2.connect(
            host="localhost", port=5432, 
            user="admin", password=db_password, dbname="vision_db"
        )
        cursor = conn.cursor()
        
        cursor.execute("SELECT SUM(car_count), SUM(person_count) FROM traffic_logs;")
        result = cursor.fetchone()
        
        total_cars = result[0] if result[0] else 0
        total_persons = result[1] if result[1] else 0
        
        conn.close()
        
        return {
            "status": "SUCCESS", 
            "total_cars": total_cars,
            "total_persons": total_persons
        }
    except Exception as e:
        return {"status": "ERROR", "message": str(e)}
