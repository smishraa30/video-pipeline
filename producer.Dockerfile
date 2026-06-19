FROM python:3.10-slim
WORKDIR /app
RUN apt-get update && apt-get install -y libgl1-mesa-glx libglib2.0-0
RUN pip install fastapi uvicorn kafka-python opencv-python
COPY producer.py test_video.mp4 ./
CMD ["uvicorn", "producer:app", "--host", "0.0.0.0", "--port", "8000"]

RUN pip install fastapi uvicorn kafka-python opencv-python-headless psycopg2-binary python-dotenv

COPY producer.py test_video.mp4 .env ./

CMD ["uvicorn", "producer:app", "--host", "0.0.0.0", "--port", "8000"]
