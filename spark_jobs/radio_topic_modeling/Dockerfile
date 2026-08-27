# Custom container image for a Google Cloud Managed Service for Apache Spark
# (Managed Spark Serverless) batch. faster-whisper/BERTopic/torch are far too
# heavy to `pip install` on every batch run, so their weights and deps are
# baked in here at build time instead. See README.md for how to build, push,
# and submit this image.
FROM debian:12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        procps tini libgomp1 python3 python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/*

# Managed Spark runs containers as the fixed `spark` user (uid/gid 1099).
RUN groupadd -g 1099 spark \
    && useradd -u 1099 -g 1099 -d /home/spark -m spark

ENV PYSPARK_PYTHON=/opt/venv/bin/python \
    PATH="/opt/venv/bin:${PATH}"

RUN python3 -m venv /opt/venv
COPY requirements.txt /opt/radio_topic_modeling/requirements.txt
RUN /opt/venv/bin/pip install --no-cache-dir -r /opt/radio_topic_modeling/requirements.txt

# Pre-download model weights at build time so batch startup doesn't re-fetch
# them (and doesn't depend on outbound internet access from the executors).
ARG WHISPER_MODEL_SIZE=base
ARG EMBEDDING_MODEL_NAME=all-MiniLM-L6-v2
RUN /opt/venv/bin/python -c "from faster_whisper import WhisperModel; WhisperModel('${WHISPER_MODEL_SIZE}', device='cpu', compute_type='int8')" \
    && /opt/venv/bin/python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('${EMBEDDING_MODEL_NAME}')"

COPY job.py /opt/radio_topic_modeling/job.py

RUN chown -R spark:spark /opt/radio_topic_modeling /opt/venv /home/spark
USER spark
WORKDIR /home/spark
