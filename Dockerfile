FROM flink:1.18-java11

RUN wget -P /opt/flink/lib/ \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

RUN apt-get update && \
    apt-get install -y python3 python3-pip default-jdk-headless && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt
