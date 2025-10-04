FROM openjdk:11-jre-slim

ARG SPARK_VERSION=3.5.1
ARG HADOOP_VERSION=3
ARG SPARK_BASE_URL=https://archive.apache.org/dist/spark
ARG SPARK_TGZ=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Dependências básicas + Python para rodar PySpark nos workers
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl bash python3 python3-pip procps tini ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# (Opcional) manter PySpark alinhado; a distribuição já inclui, mas o python precisa existir
RUN pip3 install --no-cache-dir pyspark==${SPARK_VERSION}

# Instala o Spark
RUN mkdir -p /opt && \
    curl -fsSL ${SPARK_BASE_URL}/spark-${SPARK_VERSION}/${SPARK_TGZ} -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm -f /tmp/spark.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

# Entrypoint que decide entre master/worker
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

EXPOSE 7077 8080 8081 8082
ENTRYPOINT [ "tini", "--", "docker-entrypoint.sh" ]
CMD [ "master" ]
