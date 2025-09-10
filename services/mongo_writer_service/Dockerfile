FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared/ ./shared/
COPY dal/ ./dal/
COPY services/mongo_writer_service/ ./services/mongo_writer_service/
ENV PYTHONPATH=/app
CMD ["python", "-m", "services.mongo_writer_service.src.main"]
