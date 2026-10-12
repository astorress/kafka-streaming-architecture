FROM python:3.13-slim-trixie

ENV TZ=America/Bogota PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y --no-install-recommends curl procps tini \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app
CMD ["sleep", "infinity"]
