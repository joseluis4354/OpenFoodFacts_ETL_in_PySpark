from sparkSession import start_sparkSession, create_sqlcontext
from extract import extract_data, open_file
from transform import *
from load import load_data




#Entry points of Spark

spark = start_sparkSession()

sqlContext = create_sqlcontext(spark)

# Extracting the data

extract_data()

FoodDatabaseDF = open_file(spark)

# Transforming the data

FoodDatabaseDF = drop_rows(FoodDatabaseDF)

FoodDatabaseDF = missing_data(FoodDatabaseDF)

FoodDatabaseDF = imputing_categ_data(FoodDatabaseDF)

FoodDatabaseDF = imputing_numeric_data(FoodDatabaseDF)


# Loading the data

load_data(FoodDatabaseDF)
