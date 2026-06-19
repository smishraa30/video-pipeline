FROM python:3.10-slim
WORKDIR /app

# 1. Force CPU-only PyTorch
RUN pip install torch torchvision --index-url https://download.pytorch.org/whl/cpu

# 2. Install YOLO and other dependencies
RUN pip install kafka-python ultralytics numpy psycopg2-binary python-dotenv

# 3. THE FIX: Uninstall the GUI OpenCV that YOLO smuggled in, and force the headless version
RUN pip uninstall -y opencv-python && pip install opencv-python-headless

COPY consumer.py .env ./

CMD ["python", "consumer.py"]