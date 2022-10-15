ARG AIRFLOW_VERSION="2.4.1"
FROM apache/airflow:${AIRFLOW_VERSION}
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark==3.0.0
USER root
RUN apt-get update -y \
 && apt-get install -y --no-install-recommends wget \
 && apt-get autoremove -yqq --purge \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*
# Install  Java
ENV JAVA_HOME /opt/java-11
COPY openjdk-11.0.1_linux-x64_bin.tar.gz .
RUN tar xvf openjdk-11.0.1_linux-x64_bin.tar.gz \
  && mv jdk-11.0.1/ ${JAVA_HOME} \
  && rm openjdk-11.0.1_linux-x64_bin.tar.gz        
# RUN export JAVA_HOME
# Install Spark
ENV SPARK_VERSION 3.3.0
ENV SPARK_HOME /opt/spark
COPY spark-3.3.0-bin-hadoop3.tgz .
RUN tar xvf spark-3.3.0-bin-hadoop3.tgz \
  && mv spark-3.3.0-bin-hadoop3/ ${SPARK_HOME} \
  && rm spark-3.3.0-bin-hadoop3.tgz
# RUN mkdir ${SPARK_HOME}/work
VOLUME ${SPARK_HOME}/work
# RUN chown -R airflow:root ${SPARK_HOME}/work
RUN chown -R airflow:root ${SPARK_HOME} ${JAVA_HOME}    
ENV PATH "${PATH}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${JAVA_HOME}/bin"
EXPOSE 5050 4040 5060
USER airflow
