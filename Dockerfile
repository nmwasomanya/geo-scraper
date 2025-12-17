FROM python:3.10-slim

WORKDIR /app

# Install system dependencies (needed for psycopg2 and others)
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default command (can be overridden in docker-compose)
CMD ["python", "main.py", "--help"]
