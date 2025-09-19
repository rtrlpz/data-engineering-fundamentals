# -- Imports -- #
import pandas as pd
from sqlalchemy import create_engine, text
from time import time
import argparse
import os 


def main(params):
    # Get params
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    
    csv_name = url

    # Download the csv file
    # os.system(f"wget {url} -O {csv_name}")

    # Create connection
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # 1. Read the csv file
    # Data partitioning
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000, low_memory=False)
    df = next(df_iter)
    
    # 2. Create the table
    # Select the header to create columns without rows 
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    print('Table created successfully!')

    # 3. Ingest data into the table
    while True:
        try:
            # Measure time            
            t_start = time()

            # Convert to datetime
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            # Insert data into the table
            df.to_sql(name=table_name, con=engine, if_exists='append')
            
            # Get the next chunk
            df = next(df_iter)

            # End measure time    
            t_end = time()

            print('Inserted a chunk..., took %.3f seconds' % (t_end - t_start))

        except StopIteration:
            print('✅ All chunks processed and inserted into PostgreSQL.')
            
            break



if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password name for postgres')
    parser.add_argument('--host', help='host name for postgres')
    parser.add_argument('--port', help='port name for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)
    

    