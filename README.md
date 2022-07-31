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


| Table      | Field | Type | Description |
| -----------| ----- | ---- | ----------- |
| fact_immigrations | immigrations_id | INT4 | ID of the immigrant (PK) |
| fact_immigrations | origin  | INT4 | Origin country code of the immigrant |
| fact_immigrations | landing_port  | VARCHAR | IATA code of the landing port of the immigrant |
| fact_immigrations | arrival_date  | FLOAT8 | Arrival date of the immigrant in SAS format |
| fact_immigrations | departure_date  | FLOAT8 | Departure date of the immigrant in SAS format |
| fact_immigrations | arrival_mode  | INT4 | 1 ='Air', 2 = 'Sea', 3 = 'Land', 9 = 'Not reported'|
| fact_immigrations | city  | VARCHAR | City where the immigrant will stay |
| fact_immigrations | age   | INT4 | Age of the immigrant |
| fact_immigrations | visa  | INT4 | Visa code: 1 = Business, 2 = Pleasure, 3 = Student |
| fact_immigrations | visatype | VARCHAR | Class of admission legally admitting the non-immigrant to temporarily stay in U.S. |
| fact_immigrations | gender  | VARCHAR | Gender of the immigrant |
|-|-|-|-|
| dim_cities  | city | VARCHAR | Name of the city (PK) |
| dim_cities  | state | VARCHAR | Name of the state |
| dim_cities  | median_age | FLOAT8 | Median age of the city |
| dim_cities  | male_pop  | INT4 | Male population |
| dim_cities  | female_pop  | INT4 | Femala population |
| dim_cities  | total_pop  | INT4 | Total population |
| dim_cities  | veterans  | INT4 | Number of veterans |
| dim_cities  | foreign_born  | INT4 | Number of foreign-borns |
| dim_cities  | avg_household_size  | FLOAT8 | City name |
|-|-|-|-|
| dim_ports  | port_id  | VARCHAR | Unique port identifier (PK) |
| dim_ports  | name   | VARCHAR | Name of the port |
| dim_ports  | city   | VARCHAR | City where port is in |
| dim_ports  | type   | VARCHAR | Airport, heliport or seaplane base |
| dim_ports  | iata_code   | VARCHAR | IATA code of the port |
| dim_ports  | iso_region   | VARCHAR | State code where the port is in |
|-|-|-|-|
| dim_temperatures  | city | VARCHAR | City name (PK) |
| dim_temperatures  | avg_temp | FLOAT8 | Average temperature |
| dim_temperatures  | avg_temp_uncertainty | FLOAT8 | Average temperature uncertainty |
|-|-|-|-|
| dim_time  | sas_timestamp | FLOAT8 | Timestamp in SAS format (PK) |
| dim_time  | year | INT4 | year |
| dim_time  | month | INT4 | month |
| dim_time  | day | INT4 | day |
| dim_time  | week  | INT4 | week number of the sas_timestamp |
| dim_time  | weekday | INT4 | 1=Sunday, 2=Monday, 3=Tuesday, 4=Wednesday, 5=Thursday, 6=Friday, 7=Saturday |


## Author

[Arda Aras](https://www.linkedin.com/in/arda-aras/)

