FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY collector.py .

# -u : unbuffered output so logs show up in docker logs immediately
CMD ["python", "-u", "collector.py"]
