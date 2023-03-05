# MyTestingProject
This is an airflow code written in python. 
This module connects to different data sources such as oracle, DB2, DynamoDB and then loads the raw data to s3
From S3 , we perform ETL using snowflake and load the data back to s3(on top of which snowflake external tables are created)