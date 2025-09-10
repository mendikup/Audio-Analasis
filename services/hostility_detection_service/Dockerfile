FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared/ ./shared/
COPY dal/ ./dal/
COPY services/hostility_detection_service/ ./services/hostility_detection_service/
ENV PYTHONPATH=/app
CMD ["python", "-m", "services.hostility_detection_service.src.main"]
