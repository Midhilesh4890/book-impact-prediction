# Base image with Bitnami Spark
FROM bitnami/spark:3.3.1

# Install Python and system dependencies
USER 0
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-dev \
    libffi-dev \
    gcc \
    g++ && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Upgrade pip and setuptools
RUN pip3 install --upgrade pip setuptools wheel

# Copy the Python application files
WORKDIR /app
COPY requirements.txt /app/

# Install Python dependencies
RUN pip3 install -r requirements.txt

# Copy application code
COPY . /app/

# Expose ports for Spark
EXPOSE 8080 7077 6066

# Ensure the /app directory is writable
RUN mkdir -p /app/logs && chmod -R 777 /app


# Switch back to the non-root user
USER 1001

# Default command to start the application
CMD ["spark-submit", "--master", "local[2]", "main.py"]
