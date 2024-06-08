# Use an appropriate base image
FROM python:3.10-slim

# Install Java 17 and procps
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless procps && \
    apt-get clean;

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64

# Set the working directory
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt

# Entry point for your application
CMD ["python", "scripts/main.py"]
