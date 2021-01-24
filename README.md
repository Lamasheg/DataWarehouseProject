# DataWarehouseProject


### Table of Contents

1. [Project Overview](#summary)
2. [File Structure](#Files)
3. [Data Schema](#schema)
4. [Instructions to run the files](#inst)
5. [Acknowledgements](#licensing)


    
## Project Overview<a name="summary"></a>

This project is part of Data Engineering Nano degree, which mainly focuses on a music streaming startup, Sparkify, 
that want to move their processes and data onto the cloud. 
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project we build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables
for their analytics team to continue finding insights in what songs their users are listening to. 

## File Structure<a name="Files"></a>

1. Setup Redshift Cluster.ipynb: creates all required paramteres to eventually create the Redshift Cluster (creates dc2.large with 4 nodes)
2. create_tables.py: runs scripts in sql_queries.py to create staging and analytics tables
3. dwh.cfg: holds our AWS user, role, cluster, DB info
4. etl.py: runs scripts in sql_queries.py to extract, transforma and load data to our database tables
5. sql_queries.py: scripts to drop / create tables and insert data into tables


## Data Schema <a name="schema"></a>

following is a diagram that dipicts fact and dimension tables to represent a star schema:
![](sparkifySchema.png?raw=true)
            

## Instructions to run the files<a name="inst"></a>

1. follow instructions in setup Redshift Cluster.ipynb file to create redshift cluster
2. maintain all parameters in dwh.cfg
3. open terminal and navigate to project folder
4. run create_tables.py on terminal to tables in our cluster
5. run etl.py on terminal to extract, transforma and load data to our database tables

## Acknowledgements<a name="licensing"></a>
Credits go to udacity for providing the opportunity to practice our skills!
