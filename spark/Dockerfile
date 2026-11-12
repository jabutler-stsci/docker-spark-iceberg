# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# syntax=docker/dockerfile:1
FROM python:3.14-bookworm

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      unzip \
      openjdk-17-jdk \
      build-essential \
      software-properties-common \
      ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Jupyter and other python deps
COPY requirements.txt .
RUN pip3 install --only-binary=pyiceberg,pyarrow,numpy,pandas,scipy,matplotlib,grpcio -r requirements.txt

# Optional env variables
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-src.zip

WORKDIR ${SPARK_HOME}

ENV SPARK_VERSION=4.1.4
ENV SPARK_SHA512=1461b6c425d290913e7be97bfec738abc843c663d177dc57e32b82874d20144ed67dcca692a57efc4e5b40d2f001ee0e9e0d2d7b088ae58f5378ecac92739076
ENV SPARK_MAJOR_VERSION=4.1
ENV SCALA_BINARY_VERSION=2.13
ENV ICEBERG_VERSION=1.12.0

# Download spark
RUN mkdir -p ${SPARK_HOME} \
 && curl -fL "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" -o spark-${SPARK_VERSION}-bin-hadoop3.tgz \
 && echo "${SPARK_SHA512}  spark-${SPARK_VERSION}-bin-hadoop3.tgz" | sha512sum -c - \
 && tar xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
 && ln -s "$(basename "$(find "${SPARK_HOME}/python/lib" -name 'py4j-*-src.zip' -print -quit)")" "${SPARK_HOME}/python/lib/py4j-src.zip" \
 && rm -f spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Download iceberg spark runtime
RUN curl -fL https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_${SCALA_BINARY_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_${SCALA_BINARY_VERSION}-${ICEBERG_VERSION}.jar -o /opt/spark/jars/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_${SCALA_BINARY_VERSION}-${ICEBERG_VERSION}.jar \
 && unzip -t /opt/spark/jars/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_${SCALA_BINARY_VERSION}-${ICEBERG_VERSION}.jar > /dev/null

# Download AWS bundle
RUN curl -fL https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar -o /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
 && unzip -t /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar > /dev/null

# Download GCP bundle
RUN curl -fL https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-gcp-bundle/${ICEBERG_VERSION}/iceberg-gcp-bundle-${ICEBERG_VERSION}.jar -o /opt/spark/jars/iceberg-gcp-bundle-${ICEBERG_VERSION}.jar \
 && unzip -t /opt/spark/jars/iceberg-gcp-bundle-${ICEBERG_VERSION}.jar > /dev/null

# Download Azure bundle
RUN curl -fL https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-azure-bundle/${ICEBERG_VERSION}/iceberg-azure-bundle-${ICEBERG_VERSION}.jar -o /opt/spark/jars/iceberg-azure-bundle-${ICEBERG_VERSION}.jar \
 && unzip -t /opt/spark/jars/iceberg-azure-bundle-${ICEBERG_VERSION}.jar > /dev/null

# Install AWS CLI
ARG TARGETARCH
ENV ALMOND_VERSION=0.15.0
ENV SCALA_VERSION=2.13.17
ENV COURSIER_REPOSITORIES=https://repo.maven.apache.org/maven2
COPY scala/ ${SPARK_HOME}/notebook_kernel/
RUN case "${TARGETARCH}" in arm64) CS_ARCH=aarch64 ;; amd64) CS_ARCH=x86_64 ;; *) exit 1 ;; esac \
 && curl -fL "https://github.com/coursier/coursier/releases/download/v2.1.25/cs-${CS_ARCH}-pc-linux.gz" -o /tmp/cs.gz \
 && gzip -d /tmp/cs.gz \
 && chmod +x /tmp/cs /opt/spark/notebook_kernel/kernel.sh \
 && /tmp/cs bootstrap --standalone almond:${ALMOND_VERSION} --scala ${SCALA_VERSION} \
   --force-version org.scala-lang:scala-library:${SCALA_VERSION} \
   --force-version org.scala-lang:scala-compiler:${SCALA_VERSION} \
   --force-version org.scala-lang:scala-reflect:${SCALA_VERSION} \
   -o /opt/scala-kernel \
 && /opt/scala-kernel --install --id scala --display-name 'Scala (Spark)' \
  --jupyter-path /usr/local/share/jupyter/kernels --command /opt/spark/notebook_kernel/kernel.sh \
 && rm /tmp/cs

RUN case "${TARGETARCH}" in arm64) AWS_ARCH=aarch64 ;; amd64) AWS_ARCH=x86_64 ;; *) exit 1 ;; esac \
 && curl -fL "https://awscli.amazonaws.com/awscli-exe-linux-${AWS_ARCH}.zip" -o "awscliv2.zip" \
 && unzip awscliv2.zip \
 && sudo ./aws/install \
 && rm awscliv2.zip \
 && rm -rf aws/

RUN mkdir -p /home/iceberg/data \
 && curl -fL --retry 5 --retry-all-errors --max-time 120 'https://data.cityofnewyork.us/resource/tg4x-b46p.json?$limit=1000' -o /home/iceberg/data/nyc_film_permits.json \
 && python3 -c 'import json; data=json.load(open("/home/iceberg/data/nyc_film_permits.json")); assert isinstance(data, list) and data and all("borough" in row for row in data)' \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-04.parquet -o /home/iceberg/data/yellow_tripdata_2022-04.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-03.parquet -o /home/iceberg/data/yellow_tripdata_2022-03.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-02.parquet -o /home/iceberg/data/yellow_tripdata_2022-02.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet -o /home/iceberg/data/yellow_tripdata_2022-01.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-12.parquet -o /home/iceberg/data/yellow_tripdata_2021-12.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-11.parquet -o /home/iceberg/data/yellow_tripdata_2021-11.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-10.parquet -o /home/iceberg/data/yellow_tripdata_2021-10.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-09.parquet -o /home/iceberg/data/yellow_tripdata_2021-09.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-08.parquet -o /home/iceberg/data/yellow_tripdata_2021-08.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-07.parquet -o /home/iceberg/data/yellow_tripdata_2021-07.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-06.parquet -o /home/iceberg/data/yellow_tripdata_2021-06.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-05.parquet -o /home/iceberg/data/yellow_tripdata_2021-05.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-04.parquet -o /home/iceberg/data/yellow_tripdata_2021-04.parquet

RUN mkdir -p /home/iceberg/localwarehouse /home/iceberg/notebooks /home/iceberg/warehouse /home/iceberg/spark-events /home/iceberg
COPY notebooks/ /home/iceberg/notebooks

# Add a notebook command
RUN echo '#! /bin/sh' >> /bin/notebook \
 && echo 'export PYSPARK_DRIVER_PYTHON=jupyter-notebook' >> /bin/notebook \
 && echo "export PYSPARK_DRIVER_PYTHON_OPTS=\"--notebook-dir=/home/iceberg/notebooks --ip='*' --NotebookApp.token='' --NotebookApp.password='' --port=8888 --no-browser --allow-root\"" >> /bin/notebook \
 && echo "pyspark" >> /bin/notebook \
 && chmod u+x /bin/notebook

# Add a pyspark-notebook command (alias for notebook command for backwards-compatibility)
RUN echo '#! /bin/sh' >> /bin/pyspark-notebook \
 && echo 'export PYSPARK_DRIVER_PYTHON=jupyter-notebook' >> /bin/pyspark-notebook \
 && echo "export PYSPARK_DRIVER_PYTHON_OPTS=\"--notebook-dir=/home/iceberg/notebooks --ip='*' --NotebookApp.token='' --NotebookApp.password='' --port=8888 --no-browser --allow-root\"" >> /bin/pyspark-notebook \
 && echo "pyspark" >> /bin/pyspark-notebook \
 && chmod u+x /bin/pyspark-notebook

RUN mkdir -p /root/.ipython/profile_default/startup
COPY ipython/startup/00-prettytables.py /root/.ipython/profile_default/startup
COPY ipython/startup/README /root/.ipython/profile_default/startup

COPY spark-defaults.conf /opt/spark/conf
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

COPY .pyiceberg.yaml /root/.pyiceberg.yaml

COPY entrypoint.sh .

ENTRYPOINT ["./entrypoint.sh"]
CMD ["notebook"]
