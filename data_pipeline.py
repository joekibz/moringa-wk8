#!/usr/bin/env python
# coding: utf-8

# In[88]:


#!pip install apache-airflow-providers-postgres


# In[89]:


#import the libraries needed
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import csv
import json
from airflow.providers.postgres.hooks.postgres import PostgresHook

import logging
logging.basicConfig(filename='pipeline.log', level=logging.DEBUG)


# In[90]:


default_args = {
    'owner': 'JKibera Telecoms',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


# In[91]:


dag = DAG('data_pipeline', default_args=default_args, schedule_interval='@daily')


# In[92]:


def extract_data(**kwargs):
    
    """
    Function reads csv files on local disk | converts the from dataframe to json containing dictionary | returns the data in json format
    """
    
    try:
        # extract data from CSV files
        # load the CSV data into Pandas dataframes for later transformation
        df_cust_data=pd.read_csv('/home/customer_data.csv')
        df_order_data=pd.read_csv('/home/order_data.csv')
        df_pay_data=pd.read_csv('/home/payment_data.csv')
        
        #convert df to dictionary
        dict_cust = df_cust_data.to_dict()
        dict_order = df_order_data.to_dict()
        dict_pay = df_pay_data.to_dict()
        
        #serialize to json
        json_cust = json.dumps(dict_cust)
        json_order = json.dumps(dict_order)
        json_pay = json.dumps(dict_pay)
    
    except Exception as e:
        err = "Extract() error - "+e
        logging.debug(err)
    
    
    return json_cust,json_order,json_pay




def transform_data(**kwargs): 
    
    """
    Function transforms data | merges dataframes | drops unneeded columns | returns a json string containing dictionary of values
    """
    
    try:
        jfc,jfo,jfp = kwargs['ti'].xcom_pull(task_ids='extract_data')
        
        json_dict_dfc = json.loads(jfc)
        json_dict_dfo = json.loads(jfo)
        json_dict_dfp = json.loads(jfp)
        
        dfc = pd.DataFrame.from_dict(json_dict_dfc)
        dfo = pd.DataFrame.from_dict(json_dict_dfo)
        dfp = pd.DataFrame.from_dict(json_dict_dfp)
        
        
        # convert date fields to the correct format using pd.to_datetime
        dfc['date_of_birth'] = pd.to_datetime(dfc['date_of_birth'])
        dfo['order_date'] = pd.to_datetime(dfo['order_date'])
        dfp['payment_date'] = pd.to_datetime(dfp['payment_date'])
        
        #add json serializable date str columns
        dfc['date_of_birth_str'] = dfc['date_of_birth'].apply(lambda x: x.strftime('%Y-%m-%d'))
        dfo['order_date_str'] = dfo['order_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        dfp['payment_date_str'] = dfp['payment_date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        
        # merge customer and order dataframes on the customer_id column
        co_merge_df = pd.merge(dfc, dfo, on='customer_id')
        
        # merge payment dataframe with the merged dataframe on the order_id and customer_id columns
        pay_merge_df = pd.merge(co_merge_df, dfp, on=['customer_id', 'order_id'])
        
        # drop unnecessary columns like customer_id and order_id
        df = pay_merge_df.drop(['customer_id', 'order_id'], axis=1)
        
        # group the data by customer and aggregate the amount paid using sum
        grouped_df = df.groupby('first_name').agg({'amount': 'sum'})
        
        # create a new column to calculate the total value of orders made by each customer
        df['total_order_value'] = df.groupby('first_name')['amount'].transform('sum')
        
        # calculate the customer lifetime value using the formula CLV = (average order value) x (number of orders made per year) x (average customer lifespan) 
        # from visual inspection of merged_df - each customer has 1 order in 2023 - mean_order_value=sum_of_order, all orders made in 2023 ..hence lifespan =1yr
        df['clv'] = df['amount'] * 1 * 1
        
        #df with serializable timestamp columns 
        dfs = df.drop(['date_of_birth', 'order_date', 'payment_date'], axis=1)
        
        #convert df to dictionary
        dict_transform = dfs.to_dict()
        
        #serialize to json
        json_transform = json.dumps(dict_transform)
        
    except Exception as e:
        err = "Transform() error - "+e
        logging.debug(err)

    return json_transform
    


def load_data(**kwargs):
    
    """
    Function connects to postgres instance running on docker desktop using pg_hook 
    Transformed_data is loaded into postgres database 
    """
    
    try:
        transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_data')
        
        transformed_data = json.loads(transformed_data)

        # convert the data to a list of tuples
        result = []
        
        for key, value in transformed_data["first_name"].items():
            first_name=value,
            last_name=transformed_data["last_name"][key],
            email=transformed_data["email"][key],
            country=transformed_data["country"][key],
            gender=transformed_data["gender"][key],
            date_of_birth_str=transformed_data["date_of_birth_str"][key],
            product=transformed_data["product"][key],
            price=transformed_data["price"][key],
            order_date_str=transformed_data["order_date_str"][key],
            payment_id=transformed_data["payment_id"][key],
            amount=transformed_data["amount"][key],
            payment_date_str=transformed_data["payment_date_str"][key],
            total_order_value=transformed_data["total_order_value"][key],
            clv=transformed_data["clv"][key]
            result.append((first_name,last_name,email,country,gender,date_of_birth_str,product,price,order_date_str,payment_id,amount,payment_date_str,total_order_value,clv))
        
        columns=['first_name','last_name','email','country','gender','date_of_birth_str','product','price','order_date_str','payment_id','amount','payment_date_str','total_order_value','clv']
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_db')
        pg_hook.insert_rows(table='customer_orders', rows=result, target_fields=columns)
    
    except Exception as e:
        err = "Load() error - "+e
        logging.debug(err)
    


# In[102]:


with dag:
    
    # extract data 
    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True
    )

    # transform data 
    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )

    # load data 
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True
    )
    
     # define dependencies extract_data >> transform_data >> load_data
    extract_task >> transform_task >> load_task

   
