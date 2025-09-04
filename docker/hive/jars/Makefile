HADOOP_VERSION=3.1.2
HIVE_VERSION=3.1.2

download:
	@echo "Downloading Hadoop $(HADOOP_VERSION)..."
	@if [ -f hadoop-$(HADOOP_VERSION).tar.gz ]; then \
		echo "Hadoop file already exists."; \
	else \
		curl -LO https://archive.apache.org/dist/hadoop/common/hadoop-$(HADOOP_VERSION)/hadoop-$(HADOOP_VERSION).tar.gz; \
	fi

	@echo "Downloading Hive $(HIVE_VERSION)..."
	@if [ -f apache-hive-$(HIVE_VERSION)-bin.tar.gz ]; then \
		echo "Hive file already exists."; \
	else \
		curl -LO https://archive.apache.org/dist/hive/hive-$(HIVE_VERSION)/apache-hive-$(HIVE_VERSION)-bin.tar.gz; \
	fi

	@echo "Current files:"
	ls -lh hadoop-$(HADOOP_VERSION).tar.gz apache-hive-$(HIVE_VERSION)-bin.tar.gz