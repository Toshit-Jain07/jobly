FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    JOBLY_DB=/data/jobly.db

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app app
COPY worker worker
RUN mkdir -p /data

EXPOSE 8000
# API by default. The worker service in docker-compose.yml overrides this command.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
