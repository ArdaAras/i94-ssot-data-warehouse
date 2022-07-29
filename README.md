# I94 Single Source of Truth Data Warehouse

## Project Purpose and Scope

United States Department of Homeland Security decided to create a single source of truth database to analyze immigrant behavior. They provided a variety of datasets including I94 immigration data logs, U.S. cities demographics, travel ports, and approximate temperatures across cities.

## Technologies and Tools

![Python](https://img.shields.io/badge/Python-3.6.3-blue)
![Spark](https://img.shields.io/badge/Apache%20Spark-2.4.3-green)
![Redshift](https://img.shields.io/badge/AWS-Redshift-red)
![S3](https://img.shields.io/badge/AWS-S3-blue)

## Data Model

Star schema shall be inserted here

## Files
    1. Capstone Project Template.ipynb : This file is used to explore and clean data as well as create the pipeline.
    2. I94_SAS_Labels_Descriptions.SAS : Contains column descriptions of immigration files.
    3. SAS Data files                  : Provided within Udacity workspace. Month by month I94 immigration log files.
    4. dl.cfg                          : Contains access, secret access keys and redshift database configuration information.
    5. etl.py                          : This file contains the source code of the whole etl process.
    6. us-cities-demographics.csv      : Contains information regarding US cities demographics
    7. airport-codes_csv.csv           : Provided within Udacity workspace. Contains information regarding airports, heliports and seaplane bases throughout the world.
    8. GlobalLandTemperaturesByCity.csv: Provided within Udacity workspace. Contains timestamped temperature information of cities across the world.
    9. create_tables.sql               : Contains Amazon Redshift table creation queries.

## Data Cleaning and Considerations
* All the null values have been cleared of datasets.
* Columns with large amounts of missing values have been removed.
* Some of the columns that are by no means helpful for the scope of the project are also removed.
* Only US-related data in the temperatures data are used within the project. The temperature data remaining are averaged and grouped by city.

## Data Dictionary

TODO: Shall be inserted here as a table

| Table      | Field | Description |
| ----------- | ----------- | ---- |
| Immigrations | immigrations_id | ID of the immigrant (PK)|

## Author

[Arda Aras](https://www.linkedin.com/in/arda-aras/)

