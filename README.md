# Table of Contents
- [Project Overview](#project-overview)
- [Startup Manual](#startup-manual)
- [Airflow Pipeline Steps](#airflow-pipeline-steps)

# Project Overview

This project is a multi-service data processing platform orchestrated with Docker Compose. It integrates:
- Apache Airflow for workflow orchestration
- PostgreSQL as the relational database
- ClickHouse for analytical processing
- dbt for data modeling and transformation
- Hadoop (NameNode and DataNodes) for object storage
- Docker compose for service management

# Startup Manual



1. Ensure Docker and Docker Compose are installed.
2. Clone this repository.
3. Navigate to the project root.
4. Build and start all services:
   ```bash
   docker-compose up -d --build
   ```
5. Access the Airflow webserver at [http://localhost:8080](http://localhost:8080).
6. Monitor containers and logs as needed.

# Airflow Pipeline Steps

The Airflow pipelines are defined by DAG files in the `/airflow/dags` folder. Each DAG’s filename indicates its order and function. Below is an updated overview:

- **010 - Ingestion:**  
  *(File: 010_ingestion.py)*  
  This DAG ingests raw data from urls of Amazon Reviews data and ingests it to Hadoop file system.

- **020 - Create Schemas:**  
  *(File: 020_create_schemas.py)*
  This DAG sets up the required database schemas ClickHouse for data modeling and deduplication.

- **021 - Load Staging:**  
  *(File: 021_load_staging.py)*  
  This DAG loads the ingested data into staging tables in Clichouse in batches, preparing it for transformation.

- **030 - dbt Processing:**  
  *(File: 030_dbt_processing.py)*  
  This DAG triggers dbt models to transform, dedubplicate and clean the staged data, enabling analytics-ready datasets.

- **040 - Flush source data:**  
  *(File: 040_flush_source_data.py)*  
  This DAG loads source data from Hadoop FS to the local sytems hourly.




#### TODO 
- add environment variable which will limit the data processing
- ensure connection between clickhouse and airflow



### Future Improvements
- Add ascync processing to the ingestion DAG and staging dag to optimize the process for better throughput and latency.
- Migrate to a fully distibuted clickhouse to reduce the number of components to manage and to ensure processing scalability.
- Add more modeling according to the business requirements.
- Migrate Landing zone to S3 type of storage for better scalability
- Add data quality check frameworks like Great expectations for data profiling and analysis.