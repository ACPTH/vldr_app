FROM python:3.11-slim

# Install pdftk and system dependencies
RUN apt-get update && \
    apt-get install -y pdftk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app files
COPY . .

# Create templates directory (PDFs will be committed to git)
RUN mkdir -p templates static

# Expose port
EXPOSE 10000

# Run with gunicorn
CMD gunicorn app:app --bind 0.0.0.0:${PORT:-10000} --workers 2 --timeout 120
