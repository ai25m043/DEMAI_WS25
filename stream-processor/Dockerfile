# stream-processor/Dockerfile

FROM python:3.12.10-slim

# Install system packages for Spark + Kafka + Mongo
RUN apt-get update && apt-get install -y \
    curl gcc openjdk-17-jdk libffi-dev libsasl2-dev libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Set Java env for Spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYSPARK_PYTHON=python3

# Install uv
RUN curl -Ls https://astral.sh/uv/install.sh | bash && \
    ln -s /root/.cargo/bin/uv /usr/local/bin/uv

WORKDIR /app

COPY . .

# Install dependencies using uv
RUN uv pip install --system --no-cache-dir -r requirements.txt

CMD ["uv", "run", "spark_job.py"]
