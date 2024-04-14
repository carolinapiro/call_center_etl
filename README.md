# Introduction to the project

This project contains code to populate a DataMart with data from a CallCenter, using an ETL Process.

To develop it, the following technologies were used: Python (Pandas, SQLAlchemy, ...), PostgreSQL (in Supabase)

## Business context

The company offers micro-credits to different clients: really small businesses and people with limited access to credit, so they can finance their purchases better. 

Using the CallCenter, this company approaches and follows up on their clients. By calling these clients, they are encouraged to use the digital channels the company has made available.

## DataMart structure

![DataMart Structure](docs/DataMart_Schema.png)

# ETL Process structure

The project's repo includes:
- a notebook containing an initial exploratory analysis of the data.
- a main file, executable from a console,
- specialized classes por: connecting to the DataMart and the loading logic for each of its tables.

## Loading Logic 

In the main file:
- The control table, dm_processes, is queried to obtain the periods of time to load in each fact table (in this case, we have only one, fact_llamadas).
- Por each period:
    - The related dimensions are updated.
    - The fact table is updated.
    - The period in the control table is updated.

Dimension's load logic:
- The intermediate or stg table is truncated.
- All the source data is extracted.
- The source data is cleaned and standardized, 
- and load into the staging table.
- If the dimension has foreign keys to other dimensions:
    - Business ids from the FKs are validated
    - and they are mapped to the surrogated keys used in the dimension tables
- The data from the stg table is loaded into the dimension one, using Slowly Changing Dimension logic, with changing attributes (there are hystorical or fixed attributes in this case)

Fact's load logic:
- The intermediate or stg table is truncated.
- Source data is extracted, filtering only by the period running.
- The source data is cleaned and standardized, 
- and load into the staging table.
- For the foreign keys to dimension tables:
    - Business ids from the FKs are validated
    - and they are mapped to the surrogated keys used in the dimension tables
- The data from the stg table is loaded into the fact one, first deleting the existing data for the running period.

Aditionally, at the end of the process, additional metrics are calculate.

# Scheduling options

Currently, the repo includes only the code with the loading logic, which can be run manually using a console,
having configured a virtual environment and installed the dependencies in the requirements.txt file.

For the process to run daily, an Azure Function can be used, along with an Azure pipeline o GitHub Actions to deploy the code in the repo in the Azure Function App.

An orchestrator, such as Databricks Jobs o Airflow, can also be used. To use Airflow, some changes must be applied to the code, to create a DAG and the corresponding Tasks.