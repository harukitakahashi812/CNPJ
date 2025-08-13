FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Vercel will provide $PORT; default to 8080 locally
ENV PORT=8080
EXPOSE 8080

CMD ["sh","-c","gunicorn app:app --workers 2 --threads 4 --timeout 120 --bind 0.0.0.0:${PORT:-8080}"]

