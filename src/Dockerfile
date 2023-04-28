# Google Cloud SDK installation credits: https://stackoverflow.com/questions/74662355/docker-build-debian-11-image-with-python-and-google-cloud-sdk-in-it

FROM gcr.io/cloud-marketplace/google/debian11:latest

# Install necessary components
RUN apt-get update \
    && apt-get install -y curl \
    && apt-get install -y python3 \
    && apt-get install -y python3-pip

# Install and configure Google Cloud SDK
WORKDIR /
ENV PATH=/google-cloud-sdk/bin:$PATH
RUN export CLOUD_SDK_VERSION="410.0.0" \
    && curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz \
    && tar xzf google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz \
    && rm google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz \
    && ln -s /lib /lib64

RUN gcloud config set core/disable_usage_reporting true && \
    gcloud config set component_manager/disable_update_check true && \
    gcloud config set metrics/environment github_docker_images && \
    gcloud -q components install beta kubectl

# Configure app dependencies
WORKDIR /app

COPY requirements.txt .
COPY kafka_utils ./kafka_utils
COPY models_params ./models_params
COPY prediction ./prediction
COPY processing ./processing
COPY app.py .
COPY start.sh .
COPY config.yaml .

RUN pip install --no-cache-dir --upgrade -r requirements.txt
RUN  chmod +x start.sh

ENTRYPOINT [ "./start.sh" ]