# Data-Warehouse-Pipeline

# Table of Contents

1. **[Project Objective](#project-objective)**
2. **[Datasets Selection](#datasets-selection)**
3. **[System Architecture](#system-architecture)**
   - [Data Sources](#data-sources)
   - [Data Processing Layers](#data-processing-layers)
   - [Storage Layer](#storage-layer)
   - [Analytics and Reporting](#analytics-and-reporting)
4. **[Technologies Used](#technologies-used)**
5. **[Deployment](#deployment)**
   - [System Requirements](#system-requirements)
   - [Running the Project](#running-the-project)
   - [Monitoring](#monitoring)
6. **[Results](#results)**
8. **[Future Work](#future-work)**
9. **[Authors](#authors)**


# Project Objective
The goal of this project is to create a data warehouse utilizing Python, Duckdb, and PostgreSQL, incorporating data on complaints against financial institutions and demographic information. used Docker for containeraization, and Metabase for visualization.

# Datasets Selection

## 1.Complain Dataset
- Source: [Complaines Dataset](https://catalog.data.gov/dataset/consumer-complaint-database)
- This dataset was provided by **data.gov** it containes 4M rows (more than 4 G) and has a csv format. it containes complaints about consumer financial products and services.

## 2.Demographic Dataset
- Source: [Demographic Dataset](https://www.kaggle.com/datasets/bitrook/us-county-historical-demographics?select=us_county_demographics.json)
- This dataset was provided by **Kaggle** (more than 2 G) it's in json format. It contains information regarding the demographics of different states.


# System Architecture

The system is divided into several components, each responsible for specific tasks:

  <center>
      <img src="Images/arch.png" width="1200" />
  </center>
  
## Data Sources
- We have the data files complaines.csv and geographics.json
## 2.ETL
- Using Duckdb we convert the json file to parquet and process the data and filter it uploade it postgres table.
- Processe the csv file and uploade it to postgres tables.
## 3.Storage
- Using postgres to store the tables and create the modleing for it.
## Visualization
using Metabase and sql to create some visualization.
# Data Warehouse Architecture
The schema DataWarehouse:
  <center>
      <img src="Images/schema.png" width="1200" />
  </center>


## Running the Project
### 1. Clone the Project Repository
- Run the following command to clone the project repository:
   ```bash
    git clone https://github.com/FA3001/Data-Warehouse-Pipeline.git
    ```
- Navigate to the project directory:

  ```bash
  cd Data-Warehouse-Pipeline
  ```
- Download the data run the following command to **download the complaines.csv**
     ```bash
    wget https://files.consumerfinance.gov/ccdb/complaints.csv.zip
    ```
  Download the demographics.json from here **[Demographic Dataset](https://www.kaggle.com/datasets/bitrook/us-county-historical-demographics?select=us_county_demographics.json)**
**1.3. Start the Docker Containers**
  - Next, bring up all the services defined in the `docker-compose` file:

    ```bash
    docker-compose up -d
    ```
    ```bash
    cd airflow
    docker-compose up -d
    ```
    
   - This command starts all the necessary containers for the project
**Postgres**
- Open postgres on port 8888 like this **http://127.0.0.1/8888**
  **username and pass: admin**
- Get the host for the container
  ```bash
  docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' local_pgdb
  ```
- Create server and enter the data.
