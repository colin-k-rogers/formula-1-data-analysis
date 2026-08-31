# Custom container image for an AWS EMR Serverless (Spark) job run.
# faster-whisper/BERTopic/torch are far too heavy to `pip install` on every
# job run, so their weights and deps are baked in here at build time
# instead. See README.md for how to build, push, and submit this image.
#
# Must extend an EMR Serverless base image matching the application's
# release label (see README.md) — EMR Serverless ignores ENTRYPOINT/CMD,
# forbids touching JAVA_HOME/SPARK_HOME/HIVE_HOME/TEZ_HOME, and always runs
# the job as the fixed `hadoop` user, so USER root here is only transient.
FROM public.ecr.aws/emr-serverless/spark/emr-7.5.0:latest

USER root

# hdbscan (a BERTopic dependency) compiles a C extension at install time and
# needs Python.h to do it — EMR's base image (Amazon Linux 2023, dnf) has
# gcc but not the Python dev headers by default.
RUN dnf install -y python3-devel gcc-c++ make && dnf clean all

RUN python3 -m venv /opt/venv
COPY requirements.txt /opt/radio_topic_modeling/requirements.txt
RUN /opt/venv/bin/pip install --no-cache-dir -r /opt/radio_topic_modeling/requirements.txt

# Pre-download model weights at build time so the job doesn't re-fetch them
# at runtime — executors never need outbound internet this way; only the
# driver does, to reach the OpenF1 API (see README.md's network setup).
ARG WHISPER_MODEL_SIZE=base
ARG EMBEDDING_MODEL_NAME=all-MiniLM-L6-v2
RUN /opt/venv/bin/python -c "from faster_whisper import WhisperModel; WhisperModel('${WHISPER_MODEL_SIZE}', device='cpu', compute_type='int8')" \
    && /opt/venv/bin/python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('${EMBEDDING_MODEL_NAME}')"

COPY job.py /opt/radio_topic_modeling/job.py

RUN chown -R hadoop:hadoop /opt/radio_topic_modeling /opt/venv

# Deliberately not setting PYSPARK_PYTHON via ENV here: EMR Serverless's
# driver launcher doesn't reliably pick up a plain container ENV for it, so
# AWS's documented fix is to set spark.emr-serverless.driverEnv.PYSPARK_PYTHON
# and spark.executorEnv.PYSPARK_PYTHON at submit time instead (see
# README.md) — baking it in here would apply even to job runs that don't
# request this image's venv.
USER hadoop:hadoop
