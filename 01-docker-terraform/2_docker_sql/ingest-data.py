# -- Imports -- #

import os, sys
import pandas as pd
from pandas import DataFrame
from pandas.io.parsers import TextFileReader
from logger import get_logger
from time import time, sleep
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
# from dotenv import load_dotenv


# -- Logger setup -- #
logger = get_logger(__name__, log_file="ingestion.log")

# Validation Script
logger.info("Verifing environment variables...")
REQUIRED_VARS = [
    "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB",
    "POSTGRES_HOST", "POSTGRES_PORT", "TABLE_NAME", "PGADMIN_DEFAULT_EMAIL",
    "PGADMIN_DEFAULT_PASSWORD", "DATA_CSV", "DATA_URL" 
]

missing = [var for var in REQUIRED_VARS if not os.getenv(var)]

if missing:
    raise EnvironmentError(f"Missing required env vars: {missing}")

logger.info("Verification completed.")

# -- Functions -- #
logger.info("Checking database connectivity...")
def make_engine_with_retry(
        max_retries: int = 5,
        backoff_s: int = 5 ) -> Engine:
    """
    Attempt to create a SQLAlchemy engine with retries on failure.
    """
    attempts = 0
    while attempts < max_retries:
        try:
            logger.info(f"Connecting to DB at {os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']} as {os.environ['POSTGRES_USER']} to {os.environ['POSTGRES_DB']}")
            url = (
                f"postgresql://{os.environ['POSTGRES_USER']}:"
                f"{os.environ['POSTGRES_PASSWORD']}@"
                f"{os.environ['POSTGRES_HOST']}:"
                f"{os.environ['POSTGRES_PORT']}/"
                f"{os.environ['POSTGRES_DB']}"
            )
            # Create and test the engine
            engine: Engine = create_engine(url)
            engine.connect().close()
            logger.info("Database connection established successfully.")
            
            return engine
        
        except OperationalError as e:
            attempts += 1
            wait = backoff_s * (2 ** (attempts -1))
            logger.warning(
                "Connection failed (attempt %d/%d). %s retrying in %.1f seconds...",
                attempts, max_retries, e, wait
            )
            sleep(wait)

    logger.error("Failed to connect to Postgress after %d attempts", max_retries)

    raise SystemExit(1)


# -- Main function -- #
def main():
    logger.info("Starting the data ingestion pipeline...")

    # Read variables directly from enviroment
    table_name = os.environ["TABLE_NAME"]
    csv_name = os.environ["DATA_CSV"]
    url = os.environ["DATA_URL"]

    # Download the data if it doesn't exist
    if not os.path.exists(csv_name):
        # Use -O to specify the output filename for wget
        os.system(f"wget {url} -O {csv_name}")
    logger.info("Parameters loaded from environment variables.")
    
    # Create connection
    # This single function call replaces the old user, password, host, etc.
    # variables and the direct create_engine call.
    logger.info("Creating database engine...")
    engine = make_engine_with_retry()
    logger.info("Database created succesfully.")

    # Data partitioning
    logger.info("Starting data partitioning...")
    df_iter: TextFileReader = pd.read_csv( # type: ignore # Pylance overload issue; returns TextFileReader
        csv_name, 
        iterator=True, 
        chunksize=100000
        )
    
    df: DataFrame = next(df_iter)
    logger.info("Data partitioning completed.")
   
    # Create table
    logger.info(f"Creating table {table_name} if not exists...")
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    logger.info(f"Table {table_name} is ready.")

    # Ingest data into the table
    logger.info("Starting data ingestion...")
    while True:
        try:
            logger.info("Ingesting a new chunk...")            
            t_start = time()

            logger.info("Converting datetime columns...")
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            logger.info("Inserting chunk into the database...")
            df.to_sql(name=table_name, con=engine, if_exists='append')
            df = next(df_iter)

            t_end = time()
            logger.info(f"Chunk ingested. Took {t_end - t_start:.3f} seconds.") 

        except StopIteration:
            logger.info("Data ingestion completed successfully.")            
            break

        except OperationalError as e:
            logger.error(f"OperationalError occurred: {e}")
            sys.exit(1)


if __name__ == '__main__':
    main()
    

