# Introduction
An ETL to extract xml data from data folder, transform files to a flatten format and load them to postgresql. 

#### Setup
1. Change directories at the command line
   to be inside the `etl` folder

2. Run `docker-compose up postgres -d` to start the `postgres` image.

3. Run `pip install -r requirements.txt` to install python packages

#### How to develop locally

To connect to db:

    IDE

To run the code:

    spark-submit --packages com.databricks:spark-xml_2.12:0.18.0,org.postgresql:postgresql:42.7.4 main.py

To test code, run the following command:

    coverage run -m pytest -rP