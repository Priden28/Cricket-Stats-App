# Start from the base image with Selenium and Chrome
FROM selenium/standalone-chrome:latest

# Switch to the root user to install packages
USER root

# Set the working directory inside the container
WORKDIR /app

# Install Python, pip, and MySQL client
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    default-mysql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file and install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Create the templates and data directories
RUN mkdir -p /app/templates
RUN mkdir -p /app/data

# Copy all the necessary application files
COPY app.py .
COPY config.py .
COPY cricket_service.py .
COPY data_processor.py .
COPY database.py .
COPY web_scraper.py .
COPY analytics_service.py .
COPY templates/index.html /app/templates/

# Expose the port the Flask app will run on
EXPOSE 5000

# Switch back to the less-privileged user for security
USER seluser

# Command to run when the container starts
CMD ["python3", "app.py"]