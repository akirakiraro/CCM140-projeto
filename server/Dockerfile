FROM python:3.13-slim-bookworm

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server.py store.py protocol.py replica.py .

CMD ["python", "-u", "server.py"]
