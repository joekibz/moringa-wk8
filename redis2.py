#!/usr/bin/env python
# coding: utf-8

# In[128]:


#import the libraries needed
import pandas as pd
import psycopg2
import redis
from datetime import datetime

import logging
#Setup logger
logging.basicConfig(filename='pipeline.log', level=logging.DEBUG)


# In[119]:


# Redis Cloud Instance Information
redis_host = 'redis-13274.c9.us-east-1-4.ec2.cloud.redislabs.com'
redis_port = 13274
redis_password = 'hvAHQBclIFwdNQnKCUodbnNL3nblebde' 


# In[120]:


# Redis Client Object info
r = redis.Redis(
  host=redis_host,
  port=redis_port,
  password=redis_password,
  charset="utf-8", 
  decode_responses=True)


# In[121]:


# Postgres Database Information [running on docker on my PC]
pg_host = 'localhost'
pg_database = 'db_pipeline'
pg_user = 'postgres'
pg_password = 'siri123'


# In[129]:


def extract_data():
    
    """
    Function reads data from csv file on local disk into a dataframe and loads it to redis instance hosted at redislabs.com
    Returns the dataframe 
    """
    
    try:
        #read file from local drive
        data = pd.read_csv('customer_call_logs.csv')

        # Cache data in Redis for faster retrieval
        r.set('call_logs1', data.to_json(orient='records'))
    
    except Exception as e:
        err = "Extract() error - "+e
        logging.debug(err)
    
    return data
    


# In[131]:


def transform_data():
    
    """
    Function retrieves data from redis cache to speed up performance 
    Data preprocessing and cleaning is done - convert date column to datetime data type, remove dollar sign from call cost column
    Returns dataframe containing the processed/cleaned data
    """
    
    try:
        # Retrieve data from Redis cache
        data = pd.read_json(r.get('call_logs1'))

        #Cast call_date column from object to datetime
        data['call_date'] = pd.to_datetime(data['call_date']) 

        # Remove dollar sign from column 'call_cost'
        data['call_cost'] = data['call_cost'].str.replace('$', '')

        transformed_data = data
    
    except Exception as e:
        err = "Transform() error - "+e
        logging.debug(err)
    
    return transformed_data


# In[133]:


def load_data(transformed_data):
    
    """
    Function connects to postgres instance running on local PC 
    Transformed_data df is loaded into postgres database 
    Connection to postgres is then closed
    
    """
    
    try:
        # Connect to Postgres database
        conn = psycopg2.connect(host=pg_host, database=pg_database, user=pg_user, password=pg_password)

        # Create a cursor object
        cur = conn.cursor()

        # Create a table to store the data
        cur.execute('CREATE TABLE IF NOT EXISTS customer_call_logs (\
                     customer_id INT,\
                     call_cost_usd FLOAT,\
                     call_destination VARCHAR,\
                     call_date TIMESTAMP,\
                     call_duration_min VARCHAR\
                     )')

        # Insert the transformed data into the database
        for i, row in transformed_data.iterrows():
            cur.execute(f"INSERT INTO customer_call_logs (customer_id, call_cost_usd, call_destination, call_date, call_duration_min) VALUES ({row['customer_id']}, {row['call_cost']}, '{row['call_destination']}', '{row['call_date']}', '{row['call_duration']}')")

        # Commit the changes
        conn.commit()

        # Close the cursor and connection
        cur.close()
        conn.close()
    
    except Exception as e:
        err = "Load() error - "+e
        logging.debug(err)
        
        


# In[125]:


def data_pipeline():
    # Data pipeline function
    extract_data()
    transformed_data = transform_data()
    load_data(transformed_data)


# In[136]:


if __name__ == '__main__':
    # Run the data pipeline function
    data_pipeline()


# In[ ]:




