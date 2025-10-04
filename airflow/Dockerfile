# Airflow 2.9.x com Python 3.11 (pode fixar a versão exata que preferir)
FROM apache/airflow:2.9.3-python3.11

USER root

# Java 11 para Spark, + utilitários
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless curl ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Copia o requirements
COPY requirements.txt /requirements.txt

# Muda para o usuário airflow antes de instalar
USER airflow

RUN pip install --no-cache-dir -r /requirements.txt

# volta pro diretório padrão do airflow
WORKDIR /opt/airflow
