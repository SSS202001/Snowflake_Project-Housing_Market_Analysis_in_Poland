import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from snowflake.connector.pandas_tools import pd_writer
import time
import dask.dataframe as dd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

start_time = time.time()

# Use a custom Nominatim server (replace with your actual server details if available)
geolocator = Nominatim(user_agent="otodomprojectanalysis")
nom = Nominatim(domain='localhost:8080', scheme='http')  # This is an example. Replace with actual server details if you have any.

# Create the engine
engine = create_engine(URL(
    account='****',
    user='****',
    password='****',
    database='project',
    schema='public',
    warehouse='COMPUTE_WH'
))

def reverse_geocode(location):
    retries = 3
    for i in range(retries):
        try:
            return nom.reverse(location).raw['address']
        except Exception as e:
            logging.warning(f"Retrying ({i+1}/{retries}) after error: {e}")
            time.sleep(2)
    raise Exception("Geocoding failed after multiple retries")

try:
    with engine.connect() as conn:
        query = """
        SELECT RN, concat(latitude,',',longitude) as LOCATION
        FROM (
            SELECT RN,
                   SUBSTR(location, REGEXP_INSTR(location, ' ', 1, 4) + 1) AS LATITUDE,
                   SUBSTR(location, REGEXP_INSTR(location, ' ', 1, 1) + 1, 
                          (REGEXP_INSTR(location, ' ', 1, 2) - REGEXP_INSTR(location, ' ', 1, 1) - 1)) AS LONGITUDE
            FROM otodom_data_short_flatten 
            WHERE rn BETWEEN 1 AND 300
            ORDER BY rn
        )
        """
        logging.info("--- %s seconds ---" % (time.time() - start_time))
        
        # Read SQL query into a DataFrame
        df = pd.read_sql(query, conn)
        
        df.columns = map(lambda x: str(x).upper(), df.columns)
        
        # Convert Pandas DataFrame to Dask DataFrame
        ddf = dd.from_pandas(df, npartitions=10)
        logging.info(ddf.head(5, npartitions=-1))
        
        # Apply the geocoding function
        ddf['ADDRESS'] = ddf['LOCATION'].apply(
            lambda x: reverse_geocode(x), meta=(None, 'str')
        )
        logging.info("--- %s seconds ---" % (time.time() - start_time))
        
        # Compute the Dask DataFrame
        pandas_df = ddf.compute()
        logging.info(pandas_df.head())
        logging.info("--- %s seconds ---" % (time.time() - start_time))
        
        # Write the DataFrame to the database
        pandas_df.to_sql('otodom_data_flatten_address', con=engine, if_exists='append', index=False, chunksize=16000, method=pd_writer)
except Exception as e:
    logging.error('--- Error --- %s: %s' % (type(e).__name__, str(e)))
finally:
    engine.dispose()

logging.info("--- %s seconds ---" % (time.time() - start_time))
