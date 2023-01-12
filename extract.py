from pyspark.sql import SparkSession
from pyspark.sql.types import  StructType, StringType, IntegerType, DateType, StructField
from schema import getSchema, getTable
import os
from util import PrintCustom

def extract_file(spark: SparkSession, type: str, source: str):
    if type=="CSV":
        schemaname = os.path.basename(source)
        PrintCustom.info("Extraíndo informação " + schemaname)

        df_csv = spark.read.format("CSV").\
            options(header=None, inferSchema=True, sep=";", dateFormat="dd/MM/yyyy", encoding="windows-1252").\
            schema(getSchema(getTable(schemaname))).\
            csv(source)

        return df_csv