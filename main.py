from pyspark.sql import SparkSession
from extract import extract_file
from transformation import drop_nulls, transform_ft_vendas
from load import load
from util import PrintCustom, SettingProject
import sys

def spark_init():
    return SparkSession.builder.master("local[*]").\
        appName('EloGroup').\
        config("spark.jars", "db\jdbc\postgresql-42.5.1.jar").\
        config("spark.logConf", "true").\
        getOrCreate()


SPARK=spark_init()
HDFSPATH = SettingProject.getSettings("hdfs")

def dim():
    #Extract and save to DateFrame (df)
    try:
        df_tempo   = extract_file(SPARK, "CSV", HDFSPATH+"DM_Tempo.csv")
        df_cliente = extract_file(SPARK, "CSV", HDFSPATH+"DM_Cliente.csv")
        df_loja    = extract_file(SPARK, "CSV", HDFSPATH+"DM_Loja.csv")
        df_produto = extract_file(SPARK, "CSV", HDFSPATH+"DM_Produto.csv")
        PrintCustom.ok_extract()
    except Exception as e:
        PrintCustom.error_extract(e)

    #Transformation
    PrintCustom.info_transformation()
    df_tempo   = drop_nulls(df_tempo)
    df_cliente = drop_nulls(df_cliente)
    df_loja    = drop_nulls(df_loja)
    df_produto = drop_nulls(df_produto)
    PrintCustom.ok_transformation()

    #Load DateFrame to RDBMS
    PrintCustom.info_load()
    load("JDBC", df_tempo, "dim_tempo")
    load("JDBC", df_cliente, "dim_cliente")
    load("JDBC", df_loja, "dim_loja")
    load("JDBC", df_produto, "dim_produto")
    PrintCustom.ok_load()

def fat():
    #Extract

    #Transformation
    PrintCustom.info_transformation()
    df_vendas = transform_ft_vendas(SPARK)
    PrintCustom.ok_transformation()

    #Load DateFrame to RDBMS
    PrintCustom.info_load()
    load("JDBC", df_vendas, "ft_vendas")
    PrintCustom.ok_load()

if __name__ == '__main__':
    if len(sys.argv) > 0:
        if str(sys.argv[1]).lower()=="fat":
            fat()
        if str(sys.argv[1]).lower()=="dim":
            dim()

