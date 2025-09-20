Under development ...


The Complete Picture: A Data Ingestion Pipeline
This project is an ETL (Extract, Transform, Load) pipeline designed to run with Docker. When you run docker-compose up, it orchestrates three main services:

PostgreSQL Database (pgdatabase): A persistent database to store the final data.

pgAdmin (pgadmin): A web-based GUI to easily manage and query the database.

Python Ingestion Script (ingestion): The core of the pipeline. This service automatically:

Downloads a large CSV file from a URL.

Connects to the PostgreSQL database.

Creates a table with the correct schema.

Processes the CSV in memory-efficient chunks and inserts the data into the database table.

The entire system is configured using environment variables (in a .env file) for portability and security.


Of course. These files collectively define a complete, containerized data ingestion pipeline. Here’s a breakdown of what it does, how the pieces fit together, and some key observations.

The Complete Picture: A Data Ingestion Pipeline
This project is an ETL (Extract, Transform, Load) pipeline designed to run with Docker. When you run docker-compose up, it orchestrates three main services:

PostgreSQL Database (pgdatabase): A persistent database to store the final data.

pgAdmin (pgadmin): A web-based GUI to easily manage and query the database.

Python Ingestion Script (ingestion): The core of the pipeline. This service automatically:

Downloads a large CSV file from a URL.

Connects to the PostgreSQL database.

Creates a table with the correct schema.

Processes the CSV in memory-efficient chunks and inserts the data into the database table.

The entire system is configured using environment variables (in a .env file) for portability and security.

Breakdown of Components
Each file plays a specific role in making the pipeline work.

ingest-data.py (The Worker) 🐍
This is the main Python script that performs the ETL work.

Extraction: It downloads a CSV file using wget if it's not already present.

Transformation: It reads the CSV in chunks of 100,000 rows using Pandas. For each chunk, it converts the tpep_pickup_datetime and tpep_dropoff_datetime columns from text to proper datetime objects.

Loading: It first creates a table in PostgreSQL using the column headers from the CSV. Then, it iterates through each chunk and appends the data to the table until the entire file is processed. This chunking approach is crucial for handling files that are too large to fit into memory.

logger.py (The Scribe) 📜
This is a helper module that sets up a robust logging system. It configures the logger to print messages to both the console and a rotating log file (/logs/ingestion.log). This ensures that you have a persistent record of the ingestion process, which is essential for debugging.

Dockerfile (The Blueprint) 🏗️
This file contains the instructions to build the Docker image for the ingestion service. It defines the environment for our Python script:

Starts with a lightweight python:3.13-slim base image.

Installs necessary system (wget) and Python (pandas, sqlalchemy, psycopg2) packages.

Copies the Python scripts into the image.

Sets the ENTRYPOINT, which tells Docker to run python ingest-data.py whenever a container is started from this image.

docker-compose.yml (The Conductor) 🎶
This is the master file that defines and connects all the services.

services: It defines the three containers (pgdatabase, pgadmin, ingestion).

env_file: All services load their configuration (passwords, ports, etc.) from a single .env file.

volumes: It maps folders from your local machine into the containers. This is used to persist database data (./ny_tripdata_database), pgAdmin settings (./pgadmin), and make data (./data) and logs (./logs) accessible from outside the container.

networks: It creates a dedicated virtual network (pg-network) so the containers can easily find and communicate with each other using their service names (e.g., the ingestion script connects to the host named pgdatabase).

.gitignore (The Janitor) 🧹
This file tells Git which files and folders to ignore. It's configured to exclude sensitive information (.env files), large data files (*.csv), logs, and environment-specific files (__pycache__), keeping the source code repository clean and lightweight.

