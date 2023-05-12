

from pyspark.sql import SQLContext
from pyspark.sql import SparkSession


def start_sparkSession():

    spark = SparkSession\
    .builder\
    .master('local')\
    .appName('food_data_pipeline')\
    .config("spark.driver.extraClassPath", "/home/joseluis/driver_jdbc/postgresql-42.6.0.jar")\
    .getOrCreate()

    

    print("Spark object is created...")

    return spark


def create_sqlcontext(spark):

    sqlContext = SQLContext(spark)

    return sqlContext


