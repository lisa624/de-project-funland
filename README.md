
# Funland ETL Data Platform 
**End-to-End Cloud Data Engineering Project**

This repository showcases a **production-style ETL data platform** 
It demonstrates how raw transactional data can be **ingested, transformed, modelled, tested, and deployed** using modern cloud data engineering best practices.

---

## Project Overview

The Funland ETL platform ingests operational data from a PostgreSQL source system and transforms it into a **star-schema analytics model** stored in AWS S3, ready for BI tools such as Tableau.

### Key Capabilities

- Incremental data extraction using timestamps  
- Cloud-native ETL using AWS Lambda and Step Functions  
- Star-schema modelling (facts + dimensions)  
- Parquet-based analytical storage  
- Fully tested transformation logic  
- Automated deployment with Terraform  
- Observability with CloudWatch & SNS alerts  

---

## Architecture

![ETL Pipeline](images/mvpro.png)

### Data Flow

1. **Extract**
   - Lambda connects to PostgreSQL using credentials from AWS Secrets Manager
   - Incremental extracts driven by `last_checked` timestamp (SSM Parameter Store)
   - Raw CSV files written to the S3 *Ingestion* bucket

2. **Transform**
   - Lambda transforms raw CSVs into clean dimension & fact tables
   - Data modelled into a **star schema**
   - Outputs written as **Parquet** to the S3 *Processed* bucket

3. **Orchestration**
   - AWS Step Functions coordinate extract → transform
   - Failures trigger SNS email alerts

4. **Analytics**
   - Processed data consumed by Tableau dashboards

---

## Tech Stack

<p align="center">
  <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/python/python-original.svg" width="80"/>
  <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/terraform/terraform-original.svg" width="80"/>
  <img src="https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcT_6owgj8w4Bpwc1q2BNQdQ0z_LqBLw-XB0Fg&s" width="80"/>
  <img src="https://raw.githubusercontent.com/devicons/devicon/master/icons/git/git-original.svg" width="80"/>
</p>

### Core Technologies

- Python 3.13  
- AWS Lambda  
- AWS S3  
- AWS Step Functions  
- AWS Secrets Manager  
- AWS Systems Manager (SSM)  
- Terraform  
- GitHub Actions  
- Tableau  

### Python Libraries

- awswrangler  
- boto3  
- pandas  
- pg8000  
- pytest  

---

## Data Modelling

The transformed data follows a **star schema** design:

### Fact Table
- `fact_sales_order`

### Dimension Tables
- `dim_date`
- `dim_currency`
- `dim_location`
- `dim_design`
- `dim_staff`
- `dim_counterparty`

This design supports fast analytical queries and BI-friendly reporting.

---
## Local Setup

```bash
git clone https://github.com/lisa624/de-project-funland.git
cd de-project-funland
python -m venv venv
source venv/bin/activate
make requirements
```

---

## Environment Variables (Local Testing Only)

Create a `.env` file (used locally only):

```env
totesys_user=your_db_username
totesys_password=your_db_password
totesys_database=totesys
totesys_host=your_db_host
totesys_port=5432
```

```bash
export PYTHONPATH=$(pwd)
```

---
## AWS Setup

```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=eu-west-2
```

---

## AWS Secrets Manager

Create a secret (example name: `totesys-readonly`) with:

```json
{
  "user": "...",
  "password": "...",
  "host": "...",
  "database": "totesys",
  "port": 5432
}
```

---

## Terraform Deployment

```bash
cd terraform
terraform init
terraform plan
terraform apply
```

---
## Running Tests

```bash
make unit-test
make run-checks
```

---

## Verification

- Check Step Functions execution
- Review CloudWatch logs
- Confirm files in S3 buckets
- Validate SNS alerts

---

## Visuals

![Map](images/Map.png)
A graphic representation showing the countries where the products are sold. The size of the dot corresponds to sales in the corresponding country.


![Sales by Country](images/CountrySales.png)
A graph showing sales for each country for the years 2023 and 2024.


![Sales by City](images/CitySales.png)
A graph showing sales for each city for the years 2023 and 2024.


![Sales by Month](images/SalesMonth.png)
A graph showing total sales by month.


## Acknowledgements

We would like to acknowledge **[Northcoders](https://www.northcoders.com/)** for providing the **Data Engineering Bootcamp**, which was instrumental in building the foundations for this project.  


We also used the following resources and tools throughout the project:
- [Pandas](https://pandas.pydata.org/docs/index.html) - For data sanitising.
- [Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) - The Amazon Web Services (AWS) SDK for Python, used extensively for interacting with AWS services.
- [Terraform](https://developer.hashicorp.com/terraform/docs) - Comprehensive and clear documentation that helped in managing infrastructure as code.
- [AWS Wrangler](https://aws-data-wrangler.readthedocs.io/en/stable/) - A Python library that made working with AWS data services much easier.


## Authors

- [@Leda909](https://github.com/Leda909)
- [@lisa624](https://github.com/lisa624)
- [@sapkotahari](https://github.com/sapkotahari)
- [@sarah-larkin](https://github.com/sarah-larkin)
- [@shayanshater](https://github.com/shayanshater)

