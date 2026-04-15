# healthcare-claims-data-engineering
End-to-end data engineering pipeline using Medallion Architecture (Bronze, Silver, Gold)

# Healthcare Claims Data Engineering Pipeline

## Overview
Built an end-to-end data pipeline using Medallion Architecture on Databricks.

## Architecture
Bronze → Raw data ingestion  
Silver → Data cleaning, deduplication  
Gold → Fact & Dimension tables with SCD Type 2  

## Tech Stack
- PySpark
- Delta Lake
- Azure Data Lake Storage
- Databricks

## Data Model
- Fact Table: fact_claims (claim line level)
- Dimensions:
  - dim_member
  - dim_provider
  - dim_policy
  - dim_date

## Key Features
- Window-based deduplication
- SCD Type 2 implementation
- Data Quality checks
- KPI generation

## How to Run
1. Run Bronze layer
2. Run Silver layer
3. Run Gold layer
