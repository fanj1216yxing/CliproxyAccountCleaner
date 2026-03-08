FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY CliproxyAccountCleaner.py /app/
COPY cliproxy_web_mode.py /app/
COPY config.json /app/

RUN pip install --no-cache-dir requests aiohttp

EXPOSE 8765

CMD ["python", "CliproxyAccountCleaner.py", "--host", "0.0.0.0", "--port", "8765", "--no-browser"]

