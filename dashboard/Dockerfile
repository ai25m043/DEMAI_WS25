# dashboard/Dockerfile

FROM python:3.12.10-slim

# Install uv
RUN pip install --no-cache-dir uv

WORKDIR /app

COPY . .

# Install Python dependencies using uv
RUN uv pip install --system --no-cache-dir -r requirements.txt

EXPOSE 8501

CMD ["streamlit", "run", "main.py", "--server.port=8501", "--server.address=0.0.0.0"]
