#!/bin/bash

gcloud components update # must be executed as a final configuration of Google Cloud SDK

streamlit run ./app.py &
python3 kafka_utils/consumer.py &

wait -n

exit $?