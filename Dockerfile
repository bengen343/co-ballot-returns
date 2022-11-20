# syntax=docker/dockerfile:1

FROM python:3.8-slim-buster

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY config.py analyze_co_returns.py extract_from_sos.py fetch_from_sos.py load_to_gcp.py main.py transform_co_returns.py ./

# CMD [ "python3", "./main.py" ]
# Run the web service on container startup. Here we use the gunicorn
# webserver, with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
# Timeout is set to 0 to disable the timeouts of the workers to allow Cloud Run to handle instance scaling.
CMD exec gunicorn --bind :8080 --workers 1 --threads 1 --timeout 0 main:app