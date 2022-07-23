# I94 Single Source of Truth Data Warehouse

## Project purpose and scope

United States Department of Homeland Security decided to create a single source of truth database to analyze immigrant behavior. They provided a variety of datasets including I94 immigration data logs, U.S. cities demographics, travel ports, and approximate temperatures across cities.

## Technologies and tools

![Python](https://img.shields.io/badge/Python-3.6.3-blue)
![Spark](https://img.shields.io/badge/Apache%20Spark-2.4.3-green)
![Redshift](https://img.shields.io/badge/AWS-Redshift-red)
![S3](https://img.shields.io/badge/AWS-S3-blue)

## Data model

Star schema shall be inserted here

## Files
    1. Capstone Project Template.ipynb : This file is used to explore and clean data as well as create the pipeline.
    2. I94_SAS_Labels_Descriptions.SAS : Contains column descriptions of immigration files.
    3. SAS Data files                  : Provided within Udacity workspace. Month by month I94 immigration log files.
    4. dl.cfg                          : Contains access and secret access keys for AWS.
    5. etl.py                          : This file contains the source code of the whole etl process.
    6. us-cities-demographics.csv      : Contains information regarding US cities demographics
    7. airport-codes_csv.csv           : Provided within Udacity workspace. Contains information regarding airports, heliports and seaplane bases throughout the world.
    8. GlobalLandTemperaturesByCity.csv: Provided within Udacity workspace. Contains timestamped temperature information of cities across the world.

## Considerations
* Unnamed column in immigration dataset will be dropped since there is no information about it.
* Non-US data in the temperatures dataset will be cleaned.

*Note: See the data exploration and cleaning section for further details.*

## Author

[Arda Aras](https://www.linkedin.com/in/arda-aras/)

