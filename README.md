# Restaurent Data Loader

## Description
This script loads restaurant data from a delimited text file into Snowflake, 
after performing necessary data cleaning and normalization.

## Details 
for detail please refere to attached document file Detailed Documentation.docx

## Prerequisites
- Python 3.x
- PySpark
- Snowflake Connector for Python
- Snowflake JDBC and Spark Snowflake Connector

##  Installation
-Clone the repository.
-Install dependencies (pip install -r requirements.txt)

## Data Process 
1) Requirement 1 (Json Data Process)
    - this is a 2 process step 
        -- Convert Json Data in CSV format (jsonDataConversion.py)
        -- Process this csv data into snowflake (processRestaurentData.py)
2) Requirement 2 (SQLlite database to MS SQL Database)
    -- run ProecssReview.py