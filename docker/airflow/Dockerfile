FROM apache/airflow:2.7.3-python3.10

USER root

# Install prerequisites
RUN apt-get update && \
    apt-get install -y openssh-client sshpass && \
    rm -rf /var/lib/apt/lists/*

    
USER airflow

RUN pip install --no-cache-dir \
    pandas==2.1.0 \
    confluent-kafka==2.2.0 \
    apache-airflow-providers-ssh==3.2.0


# Generate private key & public key for SSH connections
# Airflow is client, so it will keep private key
# Spark is server, so it will keep public key
RUN ssh-keygen -t ecdsa -b 521 -f /home/airflow/.ssh/id_ecdsa -N ''