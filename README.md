# moringa-wk8 -- Data pipelines with Airflow | Redis | Cassandra | Neo4j


<h3>Monday Project - Data pipelines with Airflow</h3> [data_pipeline.py]
Best practises used during the implementation

<br>1- <b>Naming convention</b>:
DAG and task names clearly describe what the workflow and tasks are meant to do
<br>2- <b>Running the latest airflow version on docker desktop</b>:
Am using the latest version of Airflow to take advantage of the newest features and bug fixes.
<br>3- <b>Logging and Monitoring</b>: 
Am using Airflow UI's built-in logging and monitoring features to monitor DAGs and tasks for errors and performance issues. 

Recommendations for deployment and running the pipeline with a cloud-based provider

1- <b>Check the reputation and track record of the cloud provider</b>: Check the reputation and track record of the cloud provider. Look at factors such as the reliability and availability of the services, the level of customer satisfaction, and the level of innovation provided.
<br>2- <b>Evaluate the offerings of different cloud providers</b>: Look at factors such as the availability of services, the level of security provided, the level of support provided, and the pricing.
<br>3- <b>Consider the level of vendor lock-in</b>: Consider the level of vendor lock-in with the cloud provider. Look at factors such as the level of portability of applications and data, and the ease of switching to a different cloud provider if necessary.

Notes on my DAG environment

-On docker desktop run container for postgres db
<br>-On docker desktop run container stack for airflow - 7 containers
<br>-Upload .py file to directory $AIRFLOW_HOME on the web-server container 
<br>-Goto airflow UI on a browser window find the DAG on list, 'unpause' it and trigger it to run the tasks

-------------------------------------------------------------------------------------------------------------------------------------------------------
<h3>Tuesday Project - Data pipelines with Redis</h3> [redis2.py]
Best practises used during the implementation

<br>1- <b>Documenting the pipeline</b>: 
Comments have been generously used in the implementation.
This documentation will help when troubleshooting issues and maintaining the pipeline in future. 
It will also help when onboarding new team members [<i>in case where pipeline is being maintained by multiple engineers</i>]
<br>2- <b>Handle errors gracefully</b>: 
Implemented logging of errors,exceptions to help diagnose and resolve issues quickly.
<br>3- <b>Optimize performance</b>: 
Used redis caching to speed up data processing during the transform phase.


Recommendations for deployment and running the pipeline with a cloud-based provider

1- <b>Budget</b>: 
Choose a cloud provider that balances the solution needs and available cash budget. Some cloud providers more expensive than others.
<br>2- <b>Learning curve</b>:
Consider ease of use of each cloud provider solution. Best to select the provider whose environment is easiest to learn 
<br>3- <b>Security of the ETL pipeline</b>:
Ensure that the ETL pipeline is secure. 
This involves securing the data store, encrypting data in transit and at rest, and ensuring that only authorized users have access to the pipeline.

Every provider has its strengths and weaknesses, so evaulate each one carefully.

------------------------------------------------------------------------------------------------------------------------------------------------------
<h3>Thursday Project - Data pipelines with Neo4j</h3> [Project - Neo4j.py]

<b>Neo4j data schema:</b>
The customer_data node schema is as follows:

<br>customer_id | INTEGER,
<br>subscription_id | INTEGER,
<br>service_id | STRING,
<br>start_date | STRING,
<br>end_date | STRING,
<br>date_of_birth | STRING,
<br>subscription_price | INTEGER

<b>Transformations done:</b>
<br>-Start date and end date columns were converted from 'object' to 'datetime' datatype
<br>-Date of birth column was dropped
<br>-Null rows were dropped

<b>Postgres data schema:</b>
<br>A postgres instance was deployed on localhost running docker desktop
<br>A database 'db_pipeline' was created.
<br>A table 'telecom_data' was created in the database, below the schema 

<br>customer_id | INTEGER,
<br>subscription_id | INTEGER,
<br>service_id | VARCHAR,
<br>start_date | DATE,
<br>end_date | DATE,
<br>price | FLOAT

<b>Steps to setup the data pipeline:</b>
<br>1-At neo4j cloud, create an instance. Import telecom user data into customer_data node using the import tools on aura portal
<br>2-On local machine, deploy a postgres db instance on docker desktop
<br>3-Update the .py file with the login credentials for the neo4j source database and the destination postgres database
<br>4-Run the .py file to extract from neo4j >> transform df >> load in postgres 

