# Amazon Books ETL Pipeline with Airflow (Astronomer)

This project is an ETL (Extract, Transform, Load) pipeline that scrapes book data from Amazon and stores it in a PostgreSQL database. The pipeline is built using Apache Airflow and is managed using Astronomer to easily run and deploy locally.

## Table of Contents
- [Project Overview](#project-overview)
- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [ETL Workflow](#etl-workflow)

## Project Overview

The Amazon Books ETL pipeline is designed to automate the process of scraping book data from Amazon's search results for books related to data engineering. The data is scraped, cleaned, and stored in a PostgreSQL database.

This ETL process includes:
- **Extraction:** Scrapes book titles, authors, prices, and ratings from Amazon.
- **Transformation:** Cleans the data and removes duplicates.
- **Loading:** Loads the cleaned data into a PostgreSQL database.

## Features
- Automated daily ETL pipeline using Apache Airflow.
- Scrapes book data from Amazon (title, author, price, rating).
- Cleans and transforms the extracted data.
- Stores the transformed data in PostgreSQL.
- Managed locally using Astronomer's Airflow development tools.

## Requirements
- Docker (for running Astro)
- PostgreSQL
- Python 3.10 or later

## Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/Hassn11q/amazon-books-etl.git
   cd amazon-books-etl
2. Install Requirements:
```bash
pip install -r requirements.txt
```
4. Install Astro CLI:
Follow the installation guide to set up Astro CLI.

5. Set up a PostgreSQL Database:

- Ensure PostgreSQL is installed and running.
- Create a database for storing the book data.
- You will configure the Postgres connection later in Airflow.

6. Initialize Astro Project: Run the following command to initialize the Astronomer project:

```bash
astro dev init
```
This will create an astro project directory.

7. Configure Airflow Postgres Connection:

- Open the Airflow UI (after running the next step) and navigate to Admin > Connections.
- Create a connection with the following parameters:
- Conn Id: postgres_default
- Conn Type: Postgres
- Host: localhost
- Schema: your_database_name
- Login: your_username
- Password: your_password
- Port: 5432
8. Start the Airflow environment: Use Astro CLI to start the Airflow environment with Docker:

```bash
astro dev start
```
This will spin up the Airflow scheduler, webserver, and any other necessary components.

9. Deploy the DAG:

- Place your DAG file (amazon_books_etl.py) in the dags folder inside your Astro project directory.

- The DAG will be automatically detected by the Airflow instance running under Astro.

10. Access the Airflow UI:

- Once the Airflow environment is up, access the Airflow UI at http://localhost:8080.
- You can trigger the DAG manually or wait for it to run on its daily schedule.

## Usage 
- Trigger the DAG manually from the Airflow UI by selecting the amazon_books_etl DAG and clicking on "Trigger DAG".
The pipeline will:

- Scrape book data from Amazon.
- Clean and transform the data.
- Insert the transformed data into PostgreSQL.
- Monitor task execution through the Airflow UI.

## ETL Workflow
The DAG performs the following tasks:

- **Extract**: Scrapes Amazon search results for "data engineering books", extracting book titles, authors, prices, and ratings.
- **Transform**: Cleans the scraped data, removing duplicates and invalid entries.
- **Load**: Inserts the transformed data into a PostgreSQL database.

