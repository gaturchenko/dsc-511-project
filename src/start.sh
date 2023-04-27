#!/bin/bash

streamlit run ./app.py &
python3 kafka_utils/consumer.py &

wait -n

exit $?