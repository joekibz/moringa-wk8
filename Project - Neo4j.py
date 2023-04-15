#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Import required libraries
from neo4j import GraphDatabase,basic_auth
import pandas as pd
import psycopg2

import logging
#Setup logger
logging.basicConfig(filename='pipeline.log', level=logging.DEBUG)


# In[2]:


# Define Neo4j connection details
neo4j_uri = "neo4j+s://398cdd93.databases.neo4j.io"
neo4j_user = "neo4j"
neo4j_password = "1CvmiesqTiuOW_B8C0RrDCO_EzdTBtHfBFDg6COVz3g"


# In[3]:


# Define Postgres connection details
pg_host = 'localhost'
pg_database = 'db_pipeline'
pg_user = 'postgres'
pg_password = 'siri123'


# In[4]:


# Define Neo4j query to extract data
neo4j_query = "MATCH (c:customer_data) RETURN c"


# In[5]:


# Define function to extract data from Neo4j and return a Pandas DataFrame
def extract_data(uri, user, pwd, q):
    
    """
    Extract function that connects to Neo4j database and returns records from the customer_data node in pandas dataframe
    """
    # Connect to Neo4j
    try:
        driver = GraphDatabase.driver(uri, auth=(user, pwd))

        with driver.session() as session:
            query = q
            results = session.run(query)
            df = pd.DataFrame([dict(record['c']) for record in results])

        driver.close()
    except Exception as e:
        err = "Extract() error - "+str(e)
        logging.debug(err)
        
        
    return df


# In[6]:


# Define function to transform data
def transform_data(df):
    
    """
    Transform function that converts date columns from string to datetime format, drops unneeded column and null values 
    Function returns cleaned df
    """
    # Convert date fields to datetime objects
    try:
        
        df["start_date"] = pd.to_datetime(df["start_date"],format='%d-%m-%Y')
        df["end_date"] = pd.to_datetime(df["end_date"],format='%d-%m-%Y')

        #drop date of birth column
        df = df.drop('date_of_birth', axis=1)

        # Remove null values
        df = df.dropna()
    
    except Exception as e:
        err = "Transform() error - "+str(e)
        logging.debug(err)
     
    return df


# In[7]:


# Define function to load data into Postgres
def load_data(transformed_df):
    
    """
    Function connects to local postgres db instance running on docker desktop
    It creates table and uploads contents of transformed df into the table
    """
    # Connect to Postgres
    try:
        
        conn = psycopg2.connect(host=pg_host, database=pg_database, user=pg_user, password=pg_password)
        # Create table if it doesn't exist
        with conn.cursor() as cursor:
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS telecom_data (
                customer_id INTEGER,
                subscription_id INTEGER,
                service_id VARCHAR,
                start_date DATE,
                end_date DATE,
                price FLOAT
            )
            """)
      
            for _, row in transformed_df.iterrows():
                cursor.execute("INSERT INTO telecom_data (customer_id, subscription_id, service_id, start_date, end_date, price) VALUES (%s, %s, %s, %s, %s, %s)",
                       (row['customer_id'], row['subscription_id'], row['service_id'], row['start_date'], row['end_date'], row['subscription_price']))


        conn.commit()


        # Close the cursor and connection
        cursor.close()
        conn.close()
    
    except Exception as e:
        err = "Load() error - "+str(e)
        logging.debug(err)
        


# In[8]:


# Define main function
def main():
    # Extract data from Neo4j
    df = extract_data(neo4j_uri, neo4j_user, neo4j_password, neo4j_query)
    
    # Transform data using Pandas
    df = transform_data(df)
    
    # Load data into Postgres
    load_data(df)


# In[9]:


# Call main function
if __name__ == "__main__":
    main()

