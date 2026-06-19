# 🚦 Distributed AI Video Pipeline (Project Beta)

![Python](https://img.shields.io/badge/Python-3.10-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.100+-green)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-Event_Streaming-black)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![Docker](https://img.shields.io/badge/Docker-Containerized-blue)
![YOLOv8](https://img.shields.io/badge/AI-YOLOv8-yellow)

An enterprise-grade, event-driven microservice architecture designed to process high-throughput video streams using AI. By decoupling the video ingestion edge from the heavy neural network compute layer via Apache Kafka, this pipeline guarantees zero dropped frames and prevents backpressure during real-time object detection.

Developed by: **[Shivam Mishra/https://github.com/smishraa30]**

---

## 🏗️ Architecture Overview

Unlike monolithic Python scripts that crash or drop frames when AI inference lags behind camera FPS, this system operates as a distributed factory:

1. **Edge Ingestion (FastAPI + OpenCV):** Connects to the video stream, samples frames (reducing CPU load by 80%), compresses them to Base64, and wraps them in JSON telemetry.
2. **Message Broker (Apache Kafka):** Acts as an elastic buffer. If the AI worker takes 60ms to process a frame but the camera sends one every 30ms, Kafka queues the payload, ensuring data integrity.
3. **AI Inference Nodes (YOLOv8):** Headless Python consumers that pull from Kafka, run object detection, and extract bounding box metadata.
4. **Persistence Layer (PostgreSQL):** Stores the structured time-series data for downstream analytics.

---

## ✨ Key Engineering Features

* **Event-Driven Decoupling:** Complete separation of concerns between ingestion and processing.
* **Smart Frame Sampling:** Processes every 5th frame, optimizing compute resources without sacrificing real-world tracking accuracy.
* **Headless AI Execution:** Removed UI rendering (`cv2.imshow`) to allow pure, silent processing suitable for GUI-less cloud environments (AWS EC2 / DigitalOcean).
* **Container Orchestration:** Fully reproducible infrastructure using Docker Compose with built-in healthchecks for Zookeeper, Kafka, and PostgreSQL.
* **Graceful Shutdowns:** Engineered workers to catch `SIGINT` (Ctrl+C) signals and safely close database connections to prevent memory leaks.

---

## 🚀 Quickstart Guide

Want to run this distributed factory on your local machine?

### 1. Prerequisites
* Docker and Docker Compose installed.
* Python 3.10+ installed.

### 2. Setup Environment
Clone the repository and set up your secure environment variables:
```bash
git clone [https://github.com/yourusername/video-pipeline.git](https://github.com/yourusername/video-pipeline.git)
cd video-pipeline