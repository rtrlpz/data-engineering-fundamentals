SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (
  SELECT FROM pg_database WHERE datname = 'airflow'
)\gexec

SELECT 'CREATE DATABASE nyc_tripdata'
WHERE NOT EXISTS (
  SELECT FROM pg_database WHERE datname = 'nyc_tripdata'
)\gexec
