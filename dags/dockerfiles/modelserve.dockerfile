FROM python:3.12-slim

# Install dependencies
RUN apt-get update && apt-get install -y git build-essential && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements
COPY fastapi_requirements.txt .

RUN pip install --no-cache-dir -r fastapi_requirements.txt

# Copy code
COPY fastapi_app.py .
COPY model_manager.py .

EXPOSE 8000

# CMD ["uvicorn", "fastapi_app:app", "--host", "0.0.0.0", "--port", "8000"]

# docker build -t fastapi-ml:latest -f modelserve.dockerfile .
# docker run --rm -p 8000:8000 fastapi-ml:latest
# kubectl apply -f fast-api.yaml
# docker run --rm -p 8000:8000 fastapi-ml:latest
