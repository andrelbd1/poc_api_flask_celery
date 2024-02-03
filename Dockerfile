# For more information, please refer to https://aka.ms/vscode-docker-python
FROM python:3.8.3-slim-buster

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY . /app
COPY requirements.txt /app/requirements.txt

#RUN pip install psycopg2-binary==2.9.9

RUN pip install -r /app/requirements.txt --no-cache-dir

# CMD main.py and celery
CMD celery -A celery_worker.celery worker --loglevel=info --without-gossip --without-mingle --without-heartbeat --pool=prefork --task-events --prefetch-multiplier=1 --max-tasks-per-child=1 --time-limit=125 & python main.py