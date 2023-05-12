
from pyspark.sql.functions import *



def drop_rows(FoodDatabaseDF):

    FoodDatabaseDF = FoodDatabaseDF.na.drop(subset=["product_name","brands"])
    return FoodDatabaseDF




"""
Handling with missing values in categorical data

In the next cases, we will imput the value "missing", to treats it as a separate category

"""

def missing_data(FoodDatabaseDF):
    categories = ["categories_en","ingredients_text","additives_en","main_category_en","image_url"]

    for category in categories:
        FoodDatabaseDF = FoodDatabaseDF.na.fill(value="missing",subset=[category])
    return FoodDatabaseDF



"""
Creating a function that imputing the most repeated data on the column of origin

"""

def categorical_mode_imputing(DF, table, column_name):

    # getting the bigger value of the agrouped table countries
    max_count = table.where(col(column_name) != 'null').select(max("count")).first()[0]

    #getting the name of the country that is the most repeated 
    table_mode = table.select(column_name).where(col("count") == max_count).first()[0]

    #Filling the null fields
    DF = DF.na.fill(value = table_mode,subset=[column_name])

    return DF


def imputing_categ_data(FoodDatabaseDF):
     
  
    countries_table = FoodDatabaseDF.groupBy("countries_en").count().orderBy("count", ascending=False)
    additives_table = FoodDatabaseDF.groupBy("additives_n").count().orderBy("count", ascending=False)
    

    column_names = ["countries_en","additives_n"]
    table_names = [countries_table,additives_table]

   

    for i, column in enumerate(column_names):

        print(table_names[i], column)

        FoodDatabaseDF = categorical_mode_imputing(FoodDatabaseDF, table_names[i], column)
    
    return FoodDatabaseDF
    


"""
Creating a function that calculate the mean of a specific column

"""

def mean_numerical_tables(DF, column ):

    table = DF.where(col(column) != 'null').select(column)
    table = table.withColumn(column, col(column).cast('float'))

    total_values = table.count()
    total_sum = table.select(sum(table[column])).first()[0]
    mean = total_sum/total_values

    return mean


def imputing_mean (DF, column):

    mean = mean_numerical_tables(DF, column)
    DF = DF.withColumn(column, col(column).cast('float'))
    DF = DF.na.fill(value = mean ,subset=[column])
    
    return DF


def imputing_numeric_data(FoodDatabaseDF):

    column_list = ["energy-kcal_100g", "energy_100g", "fat_100g", "saturated-fat_100g", 
                    "carbohydrates_100g", "sugars_100g", "fiber_100g", "proteins_100g", 
                    "salt_100g", "sodium_100g"]
    
    for column in column_list:
        FoodDatabaseDF = imputing_mean(FoodDatabaseDF, column)
       
    
    return FoodDatabaseDF


