# Use Python 3.11 slim image as base
FROM python:3.11-slim-bookworm

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV FIREFLY_HOST=0.0.0.0
ENV FIREFLY_PORT=5011

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    iproute2 \
    iputils-ping \
    net-tools \
    && rm -rf /var/lib/apt/lists/*

# Set work directory
WORKDIR /app

# Copy requirements first for better layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create app user for security
RUN groupadd --gid 1000 app && \
    useradd --uid 1000 --gid 1000 --create-home --shell /bin/bash app

# Create data directory for database and set permissions
RUN mkdir -p /app/data && chown -R app:app /app

# Switch to non-root user
USER app

# Expose ports
EXPOSE 5011
EXPOSE 4403/udp

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:5011/ || exit 1

# Default command runs the Firefly application
CMD ["python3", "start.py"]