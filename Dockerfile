FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py .
COPY start.sh .
RUN chmod +x start.sh

EXPOSE 8000

ENTRYPOINT ["./start.sh"]
