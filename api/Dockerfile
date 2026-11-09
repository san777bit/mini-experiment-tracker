FROM python:3.11-slim
WORKDIR /srv
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
USER nobody
EXPOSE 8000
# Workers come from WEB_CONCURRENCY (uvicorn reads it). Several processes are safe: each has its
# own LISTEN connection and Postgres delivers NOTIFY to all of them; data always comes from the DB.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
