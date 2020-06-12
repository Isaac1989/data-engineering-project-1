# Project Background And Purpose 
A hypothetical startup, Sparkify has data about user activities on their streaming app. These data are stored in json log files. They want these information to be processed and loaded into a well design database. The resulting database will be used by the analytic team to learn about user behaviour to help drive business decision.

# Database Schema
In this project, the star schema was use due to a few reasons:
1. It simplifies queries
2. It allows for fast aggregation
3. It is denormalized which enables fewer joins for fast aggregation

# Example Query
 > `SELECT * FROM songplays WHERE duration > 100`
 
# How to Run Scripts
1. Run create_tables.py to create database and necessary tables
2. run etl.py begin ETL process.

