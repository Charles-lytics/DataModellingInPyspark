from pyspark.shell import spark
from pyspark.sql import *


def create_dimprofession():
    dim_profession = spark.sql("""
        create table patient as 
        select 
            row_number() Over(order by profession) as professionID, profession 
        from 
            (select distinct profession from patientDataDF) t""")
