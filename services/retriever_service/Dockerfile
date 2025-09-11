FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared/ ./shared/
COPY dal/ ./dal/
# Copy the retriever service instead of the mongo writer
COPY services/retriever_service/ ./services/retriever_service/
ENV PYTHONPATH=/app
# Run the retriever service main module
CMD ["python", "-m", "services.retriever_service.src.main"]
