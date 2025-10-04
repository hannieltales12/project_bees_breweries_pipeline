#!/usr/bin/env bash
set -e

SPARK_MODE="${SPARK_MODE:-master}"

if [ "$SPARK_MODE" = "master" ]; then
  : "${SPARK_MASTER_HOST:=0.0.0.0}"
  : "${SPARK_MASTER_PORT:=7077}"
  : "${SPARK_MASTER_WEBUI_PORT:=8080}"
  echo ">> Starting Spark MASTER on ${SPARK_MASTER_HOST}:${SPARK_MASTER_PORT} (UI: ${SPARK_MASTER_WEBUI_PORT})"
  exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.master.Master \
      --host "${SPARK_MASTER_HOST}" \
      --port "${SPARK_MASTER_PORT}" \
      --webui-port "${SPARK_MASTER_WEBUI_PORT}"
elif [ "$SPARK_MODE" = "worker" ]; then
  if [ -z "${SPARK_MASTER_URL}" ]; then
    echo "ERROR: defina SPARK_MASTER_URL (ex.: spark://spark-master:7077)"
    exit 1
  fi
  : "${SPARK_WORKER_CORES:=1}"
  : "${SPARK_WORKER_MEMORY:=1G}"
  : "${SPARK_WORKER_WEBUI_PORT:=8081}"
  echo ">> Starting Spark WORKER -> ${SPARK_MASTER_URL} (cores=${SPARK_WORKER_CORES}, mem=${SPARK_WORKER_MEMORY}, ui=${SPARK_WORKER_WEBUI_PORT})"
  exec "${SPARK_HOME}/bin/spark-class" org.apache.spark.deploy.worker.Worker \
      --cores "${SPARK_WORKER_CORES}" \
      --memory "${SPARK_WORKER_MEMORY}" \
      --webui-port "${SPARK_WORKER_WEBUI_PORT}" \
      "${SPARK_MASTER_URL}"
else
  echo "Usage: set SPARK_MODE=master|worker"
  exit 1
fi
