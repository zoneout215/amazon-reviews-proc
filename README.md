# Amazon Reviews Data Pipeline & Analytics

Data pipeline implementation for processing Amazon product reviews using SNAP dataset (2018):
- Reviews [source](https://snap.stanford.edu/data/amazon/productGraph/item_dedup.json.gz);
- Metadata [source](https://snap.stanford.edu/data/amazon/productGraph/metadata.json.gz);
- Source: Stanford SNAP Project (http://jmcauley.ucsd.edu/data/amazon/)

This pipeline performs ETL operations for category-based review analysis, focusing on top product categories and their performance metrics.

# Table of Contents
- `airflow/` - Airflow DAGs and plugins
- `dbt/` - dbt models and configurations
- `docker-compose.yml` - Docker Compose configuration
- `README.md` - Project overview and startup manual
- `er-diagrams/` - Entity Relationship Diagrams text and images
- `Dockerfile` - Airflow webserver Dockerfile
- `requirements.txt` - Python package requirements for Airflow
- `output.csv` - Output data mart of the top five rated categories per month with most reviewed products 

# Project Overview
This project is a multi-service data processing platform orchestrated with Docker Compose. It integrates:
- Apache Airflow for workflow orchestration
- PostgreSQL as the relational database for Airflow metadata
- BigQuery for analytical processing 
- Goolge Cloud Storage  for object storage
- DataFlow for decompression of the source data
- dbt for data modeling and transformation
- Docker compose for service management

## 0\. Software used 
| Software                      | Name and version                                       |
|-------------------------------|--------------------------------------------------------|
| Operating System              | MacOS 15.3 (version 20.04) M1                           |
| CPU configuration             | Apple M1 Pro - 16 GB RAM                               |
| Container host                | OrbStack                                               |
| Container software            | Docker (version 2.7.3) |
| Container orchestration       | Docker Compose (version 3.8)                           |
| Orchestrator software         | Apache Airflow (version 2.7.3)                           |
| Database                      | PostgreSQL (version 13)                                |
| Analytical database           | ClickHouse (version 24.10.4)                           |
| Object storage                | Hadoop (Namenode version 2.0.0-hadoop3.2.1-java8)       |
| Data transformation tool      | dbt (version 1.0.0)                                    |


## 1\. Startup Manual

1. Ensure Docker and Docker Compose are installed.
2. Install Google Cloud SDK
   Follow the instructions to install the Google Cloud SDK: https://cloud.google.com/sdk/docs/install

3. Enable Required APIs
   Enable the necessary Google Cloud APIs:
   ```sh
   gcloud services enable dataflow.googleapis.com
   gcloud services enable bigquery.googleapis.com
   gcloud services enable storage.googleapis.com
   ```
4. Clone this repository.
5. For the local test you can limit the number of batches to be processed by changing the `NUMBER_STAG_BATCHES` variable in the `local.env` file.
   ```yaml
    NUMBER_STAG_BATCHES=1
    BUCKET_NAME=<your-bucket-name>
    PROJECT_ID=<your-project-id>
    SERVICE_ACCOUNT=<your-service-account>@<your-project>.iam.gserviceaccount.com
   ```
6. Create Service Account Key
   Create a service account key for authentication:
   ```sh
   gcloud iam service-accounts keys create OUTPUT_FILE --iam-account <your-service-account>@<your-project>.iam.gserviceaccount.com
   ```

7. Build and start all services:
   ```bash
   docker-compose up -d --build
   ```


8. Monitor containers and logs as needed.

## 2. Airflow Pipeline Steps

The Airflow pipelines are defined by DAG files in the `/airflow/dags` folder. Each DAG’s filename indicates its order and function. Below is an updated overview:

- **010 - Ingestion:**  
  *(File: 010_ingestion.py)*  
  This DAG ingests raw data from urls of Amazon Reviews data and ingests it to Hadoop file system.

- **020 - Create Schemas:**  
  *(File: 020_create_schemas.py)*
  This DAG sets up the required database schemas in ClickHouse for data modeling and deduplication.

- **021 - Load Staging:**  
  *(File: 021_load_staging.py)*  
  This DAG loads the ingested data into staging tables in ClickHouse in batches, preparing it for transformation.

- **030 - dbt Processing:**  
  *(File: 030_dbt_processing.py)*  
  This DAG triggers dbt models to transform, deduplicate and clean the staged data, enabling analytics-ready datasets.

- **040 - Flush source data:**  
  *(File: 040_flush_source_data.py)*  
  This DAG loads source data from Hadoop FS to the local systems hourly.

## 3. Future Improvements
- Add asynchronous processing to the ingestion DAG and staging DAG to optimize the process for better throughput and latency.
- Migrate to a fully distibuted ClickHouse to reduce the number of components to manage and to ensure processing scalability.
- Add more modeling according to the business requirements.
- Migrate Landing layer from HDFS to S3 type of storage for better scalability and user experience.
- Add data quality check frameworks like Great expectations for data profiling and analysis.



#TODO: 
- Test run with time estimation
- Adjust schemas 
- Adjust data transformations
- Presentation
- Adjust production settings
- Adjust README.md
- Reflect on learing points 
**prepared by Sergei Romanov** 