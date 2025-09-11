# event-simulator/Dockerfile

FROM python:3.12.10-slim

# Install required system packages for aiokafka, SASL, etc.
RUN apt-get update && apt-get install -y \
    curl gcc libffi-dev libsasl2-dev libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN curl -Ls https://astral.sh/uv/install.sh | bash && \
    ln -s /root/.cargo/bin/uv /usr/local/bin/uv

WORKDIR /app

COPY . .

# Install Python dependencies using uv
RUN uv pip install --system --no-cache-dir -r requirements.txt

CMD ["uv", "run", "main.py"]
