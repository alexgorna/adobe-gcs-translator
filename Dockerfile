FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY adobe_gcs_connector.py .

CMD ["python", "adobe_gcs_connector.py"]
