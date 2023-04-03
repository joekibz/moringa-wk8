# moringa-wk8 -- Data pipelines with Airflow | Redis | Cassandra | Neo4j



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
