FROM python:3.12-slim

# Install dependencies
RUN apt-get update && apt-get install -y git build-essential && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements
COPY requirements.txt .


RUN pip install -r requirements.txt
RUN playwright install-deps
RUN playwright install
