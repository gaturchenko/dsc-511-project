FROM --platform=linux/amd64 python:3.9.5-slim-buster
WORKDIR /ltv-predictor
COPY src .
RUN pip3 install --no-cache-dir --upgrade -r requirements.txt