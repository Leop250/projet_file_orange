# Build a small image for the WSGI app
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# System deps for scientific stack (pandas/matplotlib)
RUN apt-get update \ 
    && apt-get install -y --no-install-recommends \
       build-essential \
       libfreetype6-dev \
       libpng-dev \
       pkg-config \ 
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY . .

# Cloud Run expects the service on $PORT (default 8080)
EXPOSE 8080
CMD ["gunicorn", "--bind", ":8080", "main:app"]
