from pyspark.shell import spark
from pyspark.sql import *


def create_state():
    dim_state = spark.sql("""
        create table state as 
        select 
            row_number() Over(order by state) as stateID, state 
        from 
            (select distinct state from patientDataDF) t""")
