# Overview

A music streaming company, Sparkify, has decided that it is time to introduce
more automation and monitoring to their data warehouse ETL pipelines and come to
the conclusion that the best tool to achieve this is Apache Airflow.

As a data engineer, they expect me to create high grade data pipelines that are
dynamic and built from reusable tasks, can be monitored, and allow easy backfills.
They have also noted that the data quality plays a big part when analyses are
executed on top the data warehouse and want to run tests against their datasets
after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data
warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell
about user activity in the application and JSON metadata about the songs the
users listen to.

In this project, I'll be focusing on configuring airflow environment, creating
and designing dag to automate and controle the pipeline workflow. I'll create
all the necessary tasks for the whole data pipeline.

This [link](https://github.com/mdifils/Data-Warehouse-Redshift) is my previous
project in case you want more details on building data pipeline.

# Project structure

![Project structure](images/project_structure.png)

|        File/Folder        |                       Description                       |
| :-----------------------: | :-----------------------------------------------------: |
|   Data-Pipeline-Airflow   |                    working directory                    |
|    docker-compose.yaml    |  airflow scheduler, webserver, triggerer configuration  |
| default_security_group.sh |        check default security group permissions         |
|    add_inbound_rule.sh    |            Allow airflow to access redshift             |
|    launch_redshift.sh     |          Create and configure redshift cluster          |
|    redshift_status.sh     |              Check redshift cluster status              |
|    delete_redshift.sh     |                 delete redshift cluster                 |
|     check_clusters.sh     |         check description of redshift clusters          |
|    airflow_pipeline.py    |  main script that contains dag and tasks configuration  |
|     create_tables.sql     |            SQL queries to create all tables             |
|      sql_queries.py       |   ETL to load fact and dim tables plus quality checks   |
|     stage_redshift.py     | module for custom operator to stage data into redshift  |
|       load_fact.py        | module for custom operator to load data into fact table |
|     load_dimension.py     | module for custom operator to load data into dim table  |
|      data_quality.py      |    module for custom operator to check data quality     |
