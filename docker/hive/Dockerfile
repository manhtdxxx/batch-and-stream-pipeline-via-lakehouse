FROM openjdk:8-jre


RUN apt-get update && \
    apt-get install -y curl tar netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*


# Install Apache Hadoop
ENV HADOOP_VERSION=3.1.2
ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
ENV PATH=${PATH}:${HADOOP_HOME}/bin
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop

# PICK 1 OF 2: RUN curl or COPY
# Download file locally and use COPY to save time for rebuild

# RUN curl -L "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" -o /tmp/hadoop.tar.gz
COPY jars/hadoop-${HADOOP_VERSION}.tar.gz /tmp/hadoop.tar.gz
RUN tar --no-same-owner -xvzf /tmp/hadoop.tar.gz -C /opt/ && \
    rm /tmp/hadoop.tar.gz && \
    rm -rf ${HADOOP_HOME}/share/doc
# Delete doc folder (445 MB) to reduce image size

# Change owner of folder (--no-same-owner replaces this)
# RUN chown -R root:root ${HADOOP_HOME}


# Install Apache Hive
ENV HIVE_VERSION=3.1.2
ENV HIVE_HOME=/opt/apache-hive-${HIVE_VERSION}-bin
ENV PATH=${PATH}:${HIVE_HOME}/bin
ENV HIVE_CONF_DIR=${HIVE_HOME}/conf

# RUN curl -L "https://archive.apache.org/dist/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz" -o /tmp/hive.tar.gz
COPY jars/apache-hive-${HIVE_VERSION}-bin.tar.gz /tmp/hive.tar.gz
RUN tar --no-same-owner -xvzf /tmp/hive.tar.gz -C /opt/ && \
    rm /tmp/hive.tar.gz


# Install JDBC for Hive to connect to Postgres to store metadata
ENV POSTGRES_JDBC_VERSION=42.2.24
RUN curl -L https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar -o ${HIVE_HOME}/lib/postgresql-${POSTGRES_JDBC_VERSION}.jar

# Install jars for Hive to connect S3A (MinIO)
ENV AWS_JAVA_SDK_VERSION=1.11.624
RUN curl -L "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar" -o ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aws-${HADOOP_VERSION}.jar && \
    curl -L "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_JAVA_SDK_VERSION}/aws-java-sdk-bundle-${AWS_JAVA_SDK_VERSION}.jar" -o ${HADOOP_HOME}/share/hadoop/tools/lib/aws-java-sdk-bundle-${AWS_JAVA_SDK_VERSION}.jar && \
    cp ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aws-${HADOOP_VERSION}.jar ${HIVE_HOME}/lib/ && \
    cp ${HADOOP_HOME}/share/hadoop/tools/lib/aws-java-sdk-bundle-${AWS_JAVA_SDK_VERSION}.jar ${HIVE_HOME}/lib/


# Copy config file from local to container
COPY conf/core-site.xml ${HADOOP_CONF_DIR}/core-site.xml
COPY conf/hive-site.xml ${HIVE_CONF_DIR}/hive-site.xml

# Copy entrypoint file to init Schema in DB and run Hive Metastore
COPY entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh && chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

# Expose Hive Metastore port
EXPOSE 9083

