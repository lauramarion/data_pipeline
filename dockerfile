# Dockerfile (Located in the data_pipeline/ root directory)

FROM python:3.11-slim 
WORKDIR /app 

# 1. Install Dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2. Copy the entire repository content (including the dagster/ folder)
COPY . /app

# 3. Execution: Point to the files inside the 'dagster/' subfolder
# The Code Server will run the definitions file found at /app/dagster/definitions.py
ENTRYPOINT ["dagster", "dev", "-f", "definitions.py", "--host", "0.0.0.0", "-d", "/app/dagster"]