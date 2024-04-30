from pyspark.shell import spark
from pyspark.sql import *


def create_dimgender():
    dim_gender = spark.sql("""
        create table gender as 
        select 
            row_number() Over(order by gender) as genderid, gender 
        from 
            (select distinct gender from patientDataDF) t""")
