# syntax=docker/dockerfile:1

FROM python:3.8-slim-buster

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip install --upgrade pip
RUN pip3 install -r requirements.txt

COPY config.py analyze_co_returns.py extract_from_sos.py fetch_from_sos.py load_to_gcp.py main.py transform_co_returns.py ./

CMD [ "python3", "./main.py" ]