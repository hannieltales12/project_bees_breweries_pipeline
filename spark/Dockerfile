FROM openjdk:11-jre-slim

ARG SPARK_VERSION=3.5.3
ARG HADOOP_VERSION=3
ARG SPARK_BASE_URL=https://archive.apache.org/dist/spark
ARG SPARK_TGZ=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Dependências básicas + Python para PySpark
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl bash python3 python3-pip procps tini ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Garante compatibilidade entre PySpark local e binário do Spark
RUN pip3 install --no-cache-dir pyspark==${SPARK_VERSION}

# Instala o Spark
RUN mkdir -p /opt && \
    curl -fsSL ${SPARK_BASE_URL}/spark-${SPARK_VERSION}/${SPARK_TGZ} -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm -f /tmp/spark.tgz

# Versões compatíveis com Hadoop 3.3.4
RUN mkdir -p /opt/spark/jars && \
    curl -o /opt/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -o /opt/spark/jars/aws-java-sdk-bundle-1.12.367.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Copia entrypoint genérico para Master/Worker
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

EXPOSE 7077 8080 8081 8082
ENTRYPOINT ["tini", "--", "docker-entrypoint.sh"]
CMD ["master"]
