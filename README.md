# moringa-wk8
<h3>Data pipelines with Airflow | Redis | Cassandra | Neo4j </h3>


<h3>Tuesday Project - Data pipelines with Redis</h3>
Best practises used during the implementation

1- Documenting the pipeline: 
Comments have been generously used in the implementation.
This documentation will help when troubleshooting issues and maintaining the pipeline in future. 
It will also help when onboarding new team members [in case where pipeline is being maintained by multiple engineers]
<br>2- Handle errors gracefully: 
Implemented logging of errors,exceptions to help diagnose and resolve issues quickly.
<br>3- Optimize performance: 
Used redis caching to speed up data processing during the transform phase.


Recommendations for deployment and running the pipeline with a cloud-based provider

1- Budget: 
Choose a cloud provider that balances the solution needs and available cash budget. Some cloud providers more expensive than others.
<br>2- Learning curve:
Consider ease of use of each cloud provider solution. Best to select the provider whose environment is easiest to learn 
<br>3- Security of the ETL pipeline:
Ensure that the ETL pipeline is secure. 
This involves securing the data store, encrypting data in transit and at rest, and ensuring that only authorized users have access to the pipeline.

Every provider has its strengths and weaknesses, so evaulate each one carefully.
