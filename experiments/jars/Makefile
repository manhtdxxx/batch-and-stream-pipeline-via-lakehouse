# Versions
SPARK_VERSION=3.5
SCALA_BINARY=2.12
ICEBERG_VERSION=1.5.0
HADOOP_AWS_VERSION=3.3.4
AWS_BUNDLE_VERSION=1.12.262

download:
	@echo "Downloading Iceberg Spark Runtime..."
	@if [ -f iceberg-spark-runtime-$(SPARK_VERSION)_$(SCALA_BINARY)-$(ICEBERG_VERSION).jar ]; then \
		echo "Iceberg JAR already exists."; \
	else \
		curl -L -o iceberg-spark-runtime-$(SPARK_VERSION)_$(SCALA_BINARY)-$(ICEBERG_VERSION).jar \
		https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-$(SPARK_VERSION)_$(SCALA_BINARY)/$(ICEBERG_VERSION)/iceberg-spark-runtime-$(SPARK_VERSION)_$(SCALA_BINARY)-$(ICEBERG_VERSION).jar; \
	fi

	@echo "Downloading Hadoop AWS..."
	@if [ -f hadoop-aws-$(HADOOP_AWS_VERSION).jar ]; then \
		echo "Hadoop AWS JAR already exists."; \
	else \
		curl -L -o hadoop-aws-$(HADOOP_AWS_VERSION).jar \
		https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/$(HADOOP_AWS_VERSION)/hadoop-aws-$(HADOOP_AWS_VERSION).jar; \
	fi

	@echo "Downloading AWS Java SDK Bundle..."
	@if [ -f aws-java-sdk-bundle-$(AWS_BUNDLE_VERSION).jar ]; then \
		echo "AWS SDK bundle already exists."; \
	else \
		curl -L -o aws-java-sdk-bundle-$(AWS_BUNDLE_VERSION).jar \
		https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/$(AWS_BUNDLE_VERSION)/aws-java-sdk-bundle-$(AWS_BUNDLE_VERSION).jar; \
	fi

	@echo "Spark JARs:"
	@ls -lh *jar | grep -E 'iceberg|hadoop-aws|aws-java-sdk'

clean:
	@echo "Cleaning downloaded JARs..."
	rm -f iceberg-spark-runtime-* hadoop-aws-* aws-java-sdk-bundle-*
