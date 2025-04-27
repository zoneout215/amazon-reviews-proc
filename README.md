# ETL pipeline pet project

This repository presents a ETL pipeline solution based on Amazon Reviews data. The purpose of it is mainly for practice and showcasing dataengineering approach for datatransformation. The pipeline outputs an insight about a top 5 rated categories for Amazon products for each month in the time frame of 1994 to 2014.


Data pipeline implementation for processing Amazon product reviews using SNAP dataset (2018):
- Reviews [source](https://snap.stanford.edu/data/amazon/productGraph/item_dedup.json.gz);
- Metadata [source](https://snap.stanford.edu/data/amazon/productGraph/metadata.json.gz);
- Source: Stanford SNAP Project (http://jmcauley.ucsd.edu/data/amazon/)

This pipeline performs ETL operations for category-based review analysis, focusing on top product categories and their performance metrics.

# Table of Contents
- `/airflow/` - Airflow workflow orchestration
  - `/dags/` - DAG files for data pipeline steps
  - `/plugins/` - Custom plugins and utilities
  - `/source_data/` - Local storage for processed data
- `/dbt/` - dbt models and transformations
- `/images/` - images folder with screenshots
- `Dockerfile` - Airflow webserver container definition
- `docker-compose.yml` - Multi-container orchestration
- `entrypoint.sh` - Initialization script for Airflow
- `requirements.txt` - Python dependencies
- `data_sources.tsv` - Source dataset URLs in GCS tranfer format
- `local.env` - Environment variables for local setup
- `output.csv` - Output data mart of top rated categories
- `README.md` - Project documentation

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
| Analytical database           | BigQuery (version 24.10.4)                           |
| Data transformation tool      | dbt (version 1.8.0)                                    |


## 1\. Startup Manual

1. Ensure Docker and Docker Compose are installed.
2. Install Google Cloud SDK
   Follow the instructions to install the Google Cloud SDK: https://cloud.google.com/sdk/docs/install
3. Create a GCP Project
   Create a new project in the Google Cloud Console: https://console.cloud.google.com/projectcreate. You can use free trial credits if available.
4. Set the project ID for the Google Cloud SDK:
   ```sh
   gcloud config set project <your-project-id>
   ```
5. Enable Required APIs
   Enable the necessary Google Cloud APIs:
   ```sh
   gcloud services enable dataflow.googleapis.com
   gcloud services enable bigquery.googleapis.com
   gcloud services enable storage.googleapis.com
   ```
6. Clone this repository.
7. For the local test you can limit the number of batches to be processed by changing the `NUMBER_STAG_BATCHES` variable in the `local.env` file.
   ```yaml
    BUCKET_NAME=<your-bucket-name>
    PROJECT_ID=<your-project-id>
    SERVICE_ACCOUNT=<your-service-account>@<your-project>.iam.gserviceaccount.com
   ```
8. Create Service Account Key
   Create a service account key for authentication:
   ```sh
   gcloud iam service-accounts keys create OUTPUT_FILE --iam-account <your-service-account>@<your-project>.iam.gserviceaccount.com
   ```
9. Build and start all services:
   ```bash
   docker-compose up -d --build
   ```
10. Monitor containers and logs as needed.
11. Access the Airflow web interface at `http://localhost:8080` with credentials from `entrypoint.sh` and trigger the `000_ingestion_gcs` DAG.

## 2. Airflow Pipeline Steps

The data processing pipeline consists of the following Airflow DAGs:

1. **Data Ingestion**:
   - `000_ingerstion_gcs.py`: Creates a GCS bucket, uploads data source list, initiates transfer from public URLs to GCS, waits for files to appear, then triggers decompression

> [!NOTE]  
> At this stage if the ingestion transfer is having trouble. Make sure that the service account has the necessary permissions of `Storage Object Admin`.

2. **Data Decompression**:
   - `010_decompression.py`: Uses Google Cloud Dataflow to decompress gzipped files stored in GCS, monitors for completion, then triggers staging load

3. **Data Loading**:
   - `020_load_staging.py`: Creates BigQuery dataset, loads raw data from GCS to BigQuery tables, then triggers dbt processing

4. **Data Transformation**:
   - `030_dbt_processing.py`: Runs dbt models to transform raw data into analytical tables and data marts

5. **Data Access**:
   - `040_flush_source_data.py`: Downloads processed data from GCS to local storage for further analysis

Each DAG is designed to run sequentially, with automatic triggering of the next step upon successful completion.

## 3. Future Improvements
- Add asynchronous processing to the ingestion DAG and staging DAG to optimize the process for better throughput and latency.
- Add more modeling according to the business requirements.
- Add data quality check frameworks like Great expectations for data profiling and analysis.



#TODO: 
- Test run with time estimation - 2 hours
- Presentation
- Adjust README.md 
- Reflect on learing points 
**prepared by Sergei Romanov**
