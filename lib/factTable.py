from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder.appName("CreateFactTable").getOrCreate()

# Read the rawdata table
PatientDataDF = spark.table("datasource/data.csv")

# Read the dimension tables
person_df = spark.table("dim_person")
gender_df = spark.table("dim_gender")
profession_df = spark.table("dim_profession")
state_df = spark.table("dim_state")

# Join the tables
factTable = PatientDataDF.join(person_df, PatientDataDF["name"] == person_df["name"], "inner") \
                    .join(gender_df, PatientDataDF["gender"] == gender_df["gender"], "inner") \
                    .join(profession_df, PatientDataDF["profession"] == profession_df["profession"], "inner") \
                    .join(state_df, PatientDataDF["state"] == state_df["state"], "inner") \
                    .select(
                        PatientDataDF["id"],
                        person_df["personid"],
                        gender_df["genderid"],
                        profession_df["professionID"],
                        state_df["stateID"],
                        PatientDataDF["asOfDate"],
                        PatientDataDF["temperature"],
                        PatientDataDF["pulse"]
                    )

# Create the Fact table
factTable.createOrReplaceTempView("user")

# Show the first few rows of the user table
factTable.show()

# Stop the Spark session
spark.stop()
