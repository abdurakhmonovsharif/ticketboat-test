# Use Python 3.11 slim image for better performance and smaller size
FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    python3-dev \
    libffi-dev \
    libssl-dev \
    cmake \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install -r requirements.txt --no-cache-dir

# Install New Relic APM for ECS monitoring
RUN pip install newrelic

# Copy application code
COPY src/ ./

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash app \
    && chown -R app:app /app
USER app

# Expose port 8080
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/healthcheck || exit 1

# Start command with New Relic monitoring
CMD ["newrelic-admin", "run-program", "python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]