FROM python:3.10-slim
WORKDIR /app
RUN apt-get update && apt-get install -y libgl1-mesa-glx libglib2.0-0
RUN pip install kafka-python ultralytics numpy opencv-python psycopg2-binary
COPY consumer.py ./
CMD ["python", "consumer.py"]