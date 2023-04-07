# moringa-wk8 -- Data pipelines with Airflow | Redis | Cassandra | Neo4j


<h3>Monday Project - Data pipelines with Airflow</h3>
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
<h3>Tuesday Project - Data pipelines with Redis</h3>
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
