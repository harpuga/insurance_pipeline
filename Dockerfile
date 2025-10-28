# Base Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install OS dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Ensure entrypoint scripts are executable
RUN chmod +x /app/entrypoint.sh /app/entrypoint_dashboard.sh /app/run_dashboard.sh

# Expose Streamlit port
EXPOSE 8501

# Default command (can be overridden)
ENTRYPOINT ["./entrypoint.sh"]
