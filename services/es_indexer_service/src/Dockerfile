FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared/ ./shared/
COPY dal/ ./dal/
COPY services/es_indexer_service/ ./services/es_indexer_service/
ENV PYTHONPATH=/app
CMD ["python", "-m", "services.es_indexer_service.src.main"]
