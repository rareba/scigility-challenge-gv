# Use an official PySpark image as a base
FROM apache/spark-py:v3.5.1

# Set the working directory
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application source code into the container
COPY ./src /app/src

# Set the entrypoint to run the main application script
ENTRYPOINT ["python", "src/main.py"]