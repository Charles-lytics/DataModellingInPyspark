from pyspark.shell import spark
from pyspark.sql import *


def create_dimperson():
    dim_person = spark.sql("""
        create table person as 
        select 
            row_number() Over(order by name) as personID, name 
        from
            (select distinct name from patientDataDF) t""")
