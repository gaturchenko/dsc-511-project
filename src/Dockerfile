FROM gcr.io/cloud-marketplace/google/debian11:latest

WORKDIR /app

COPY requirements.txt .
COPY kafka_utils ./kafka_utils
COPY models_params ./models_params
COPY prediction ./prediction
COPY processing ./processing
COPY app.py .
COPY start.sh .

RUN apt-get update \
    && apt-get install -y curl \
    && apt-get install -y python3 \
    && apt-get install -y python3-pip

RUN pip install --no-cache-dir --upgrade -r requirements.txt
RUN  chmod +x start.sh

ENV PATH=/google-cloud-sdk/bin:$PATH
RUN export CLOUD_SDK_VERSION="410.0.0" \
    && curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz \
    && tar xzf google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz \
    && rm google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz \
    && ln -s /lib /lib64

ENTRYPOINT ["./start.sh"]