FROM docker.io/bitnami/spark:3.3.2

USER root

# Install prerequisites
RUN apt-get update && \
    apt-get install -y curl openssh-server sudo && \
    rm -rf /var/lib/apt/lists/*

# Jars for Spark to work with Kafka, MinIO, Iceberg
WORKDIR /tmp/jars
RUN curl -L -O https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.2/spark-sql-kafka-0-10_2.12-3.3.2.jar && \
    curl -L -O https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.3.2/spark-token-provider-kafka-0-10_2.12-3.3.2.jar && \
    curl -L -O https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.2/kafka-clients-3.3.2.jar && \
    curl -L -O https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar && \
    curl -L -O https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.3.2/spark-avro_2.12-3.3.2.jar && \
    curl -L -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar && \
    curl -L -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar && \
    curl -L -O https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.5.0/iceberg-spark-runtime-3.3_2.12-1.5.0.jar && \
    mv *.jar /opt/bitnami/spark/jars/ && \
    rm -rf /tmp/jars

    
# Set user for SSH connections
RUN useradd -u 1001 -m spark_user && \
    echo spark_user:spark_pass | chpasswd && \
    usermod -aG sudo spark_user
# user 1001 is the default user for spark, but it doesn't have a name
# set password to spark_user
# add spark_user to sudo group (let it execute command as root user)


# Switch back
WORKDIR /opt/spark/app
USER spark_user