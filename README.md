# Open Food Facts ETL pipeline in PySpark

We are using the Open Food Facts database to create an ETL pipeline in PySpark

[Open Food Facts](
https://world.openfoodfacts.org/)


## Installation

* We need to have installed PySpark



## Usage
To run this ETL, we will use the spark-submit command, send the main_etl.py file as the following example:

```bash

spark-submit /home/path/of/file/main_etl.py
```
if you want to know more about spark-submit, check the next page:


[Spark Submit Command ](
https://sparkbyexamples.com/spark/spark-submit-command/?expand_article=1)

As output, we obtain a table in a PostgreSQL database with cleaned and the most important data of the database
