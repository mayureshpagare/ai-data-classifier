# Use an official Python runtime as a parent image
FROM python:3.9-slim-buster

# Set environment variables for PySpark
# Spark 3.x is compatible with Hadoop 3.2
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV PYSPARK_PYTHON=/usr/local/bin/python
ENV PYTHONPATH=/usr/lib/spark/python:/usr/lib/spark/python/lib/py4j-0.10.9-src.zip

# Install Java (required for Spark)
RUN apt-get clean && \
    apt-get update && \
    apt-get install -y default-jdk curl procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download and extract Apache Spark
RUN mkdir -p /usr/lib/spark && \
    curl -o /tmp/spark.tgz https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz && \
    tar -xzf /tmp/spark.tgz -C /usr/lib/spark --strip-components=1 && \
    rm /tmp/spark.tgz

# Set Spark home
ENV SPARK_HOME=/usr/lib/spark

# Add Spark bin directory to PATH
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# --- IMPORTANT CHANGES FOR config.py AND src/ ---
# Copy config.py and the src directory
COPY config.py .
COPY src/ src/

# Add /app to the PYTHONPATH so Python can find modules like 'config'
ENV PYTHONPATH=$PYTHONPATH:/app

# Expose any necessary ports if your application were to run a service
# For a classification script, this might not be strictly necessary unless it's a web service
# EXPOSE 8080

# Define the default command to run when the container starts
# This will execute our PySpark classifier script
CMD ["spark-submit", "src/classifier.py"]