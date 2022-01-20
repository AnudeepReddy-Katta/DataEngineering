import argparse

import os
import pandas as pd
from time import time 
from sqlalchemy import create_engine

parser = argparse.ArgumentParser(description='Ingest csv to postgres')

# user,pass, host, port, database name, table name, url of csv

def main(params):

        user = params.user
        password = params.password
        host = params.host
        port = params.port
        db = params.db
        table_name = params.table_name
        url = params.url

        csv_name = 'output.cav'

        os.system(f'wget {url} -O {csv_name}')  

        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

        engine.connect()

        df_iter = pd.read_csv(csv_name,iterator=True,chunksize=100000)

        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        while True: 
                t_start = time()

                df = next(df_iter)

                df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
                df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

                df.to_sql(name=table_name, con=engine, if_exists='append')

                t_end = time()

                print('inserted another chunk, took %.3f second' % (t_end - t_start))

if __name__ == '__main__': 
        parser.add_argument('--user',help='user name for postgres')
        parser.add_argument('--password',help='password for postgres')
        parser.add_argument('--host',help='host for postgres')
        parser.add_argument('--port',help='port for postgres')
        parser.add_argument('--db',help='db  for postgres')
        parser.add_argument('--table_name',help='table_name for postgres')
        parser.add_argument('--url',help='url for postgres')

        args = parser.parse_args()

        main(args)