import sys
import subprocess

from pyspark.sql.types import (StructType, StructField, 
                               StringType)


url = "https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv"
output_file = "/home/joseluis/Repositories/Spark/Food-pipeline/downloads/openfoodfacts-database.csv"



def download_dataset(url, output_file):
    
    try:
        subprocess.run(['wget','-O', output_file, url], check=True)
    except:
        print("Theres something wrong with the download")
        sys.exit(1)
    else:
        print("File downloaded successfully...")

    


def open_file(spark):

    customSchema = StructType([StructField("code", StringType(), True),
                            StructField("energy-kcal_100g", StringType(), True),
                            StructField("energy_100g", StringType(), True),
                            StructField("fat_100g", StringType(), True),
                            StructField("saturated-fat_100g", StringType(), True),
                            StructField("carbohydrates_100g", StringType(), True),
                            StructField("sugars_100g", StringType(), True),
                            StructField("fiber_100g", StringType(), True),
                            StructField("proteins_100g", StringType(), True),
                            StructField("salt_100g", StringType(), True),
                            StructField("sodium_100g", StringType(), True),
                            
                            ])

    openfoodfactsDF = (
        spark
        .read
        .format("csv")
        .option("header","true")
        .option("schema",customSchema)
        .option("delimiter","\t")
        .option("mode","FAILFAST")
        .load("/home/joseluis/Repositories/Spark/Food-pipeline/downloads/openfoodfacts-database.csv")
        )

    openfoodfactsDF.write.parquet("/home/joseluis/Repositories/Spark/Food-pipeline/downloads/openfoodfacts-database.parquet")

    FoodDatabaseDF = (
        spark
        .read
        .format("parquet")
        .load("/home/joseluis/Repositories/Spark/Food-pipeline/downloads/openfoodfacts-database.parquet/")
    )

    FoodDatabaseDF = FoodDatabaseDF.select("code",
    "url",
    "product_name",
    "brands",
    "categories_en",
    "countries_en",
    "ingredients_text",
    "additives_n",
    "additives_en",
    "main_category_en",
    "image_url",
    "energy-kcal_100g",
    "energy_100g",
    "fat_100g",
    "saturated-fat_100g",
    "carbohydrates_100g",
    "sugars_100g",
    "fiber_100g",
    "proteins_100g",
    "salt_100g",
    "sodium_100g",
    )

    return FoodDatabaseDF


def extract_data():

    download_dataset(url, output_file)

