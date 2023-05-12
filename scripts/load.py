import os

os.environ['DB_USER'] = "user_data"
os.environ['DB_PASSWORD'] = "admin"
os.environ['DB_HOST'] = "localhost"
os.environ['DB_PORT'] = "5432"
os.environ['DB_NAME'] = "myinner_db"

db_user = os.environ['DB_USER'] 
db_password = os.environ['DB_PASSWORD']
db_host = os.environ['DB_HOST']
db_port = os.environ['DB_PORT']
db_name = os.environ['DB_NAME']


def con():

    POSTGRES_CONFIG = {
    "url":f"jdbc:postgresql://{db_host}:{db_port}/{db_name}",
    "properties":{
        "user": db_user, 
        "password":db_password,
        "driver":"org.postgresql.Driver",
    },
}

    return POSTGRES_CONFIG

def load_data(FoodDatabaseDF):

    POSTGRES_CONFIG = con()
    FoodDatabaseDF.select("*").write.jdbc(**POSTGRES_CONFIG, table="table_food", mode="overwrite")