Project In Progress

📘 README.md – Data Engineering ETL Pipeline (CSV / JSON / XML → PostgreSQL)

📌 Overview

This repository contains a complete ETL (Extract – Transform – Load) pipeline built in Python, designed for junior Data Engineers who want to demonstrate essential data ingestion and database loading skills.

The project extracts structured and semi-structured data from CSV, JSON, and XML files, transforms it into a unified format, and loads it into a PostgreSQL database using SQLAlchemy and psycopg2.

This repository reflects a real-world data ingestion scenario and shows practical knowledge of:

- processing heterogeneous data sources
- cleaning and transforming datasets
- handling missing & inconsistent values
- building modular ETL functions
- loading structured data into Postgres

The dataset used in this project originates from Kaggle – Retail Supply Chain Sales Dataset, but it was manually split into three separate files (CSV, JSON, and XML formats) to simulate a real-world multi-source ingestion scenario.




📑 Logging System

This ETL pipeline includes a simple but effective log file system that tracks every major step of the workflow. Logging is essential for Data Engineering, as it provides transparency, debugging information, and traceability for every run of the pipeline.

This function:

- adds a timestamp
- appends human-readable messages
- tracks the start and end of each ETL phase
