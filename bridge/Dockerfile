FROM python:3.12-slim

WORKDIR /app

# install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copy app
COPY bridge.py .

# envs are supplied by docker-compose
CMD ["python", "/app/bridge.py"]
