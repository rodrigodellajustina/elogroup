from pyspark.sql import DataFrame, SparkSession, SQLContext
from pyspark.sql.functions import regexp_replace, translate, col
from schema import getSchema
from pyspark.sql import SQLContext
import configparser
from util import SettingProject

def drop_nulls(df : DataFrame):
    listcolumns = df.columns
    if "id_produto" in listcolumns:
        return df.na.drop(subset="id_produto")
    else:
        return df.dropna()



def transform_ft_vendas(spark: SparkSession, df_vendas : DataFrame, df_tempo : DataFrame, df_cliente : DataFrame, df_loja : DataFrame, df_produto : DataFrame):
    df_vendas = df_vendas.withColumn("receita_venda", translate('receita_venda', '$', ' '))
    df_vendas = df_vendas.withColumn("receita_venda", df_vendas.receita_venda.cast('numeric(12,2)'))
    #df_vendas_tmp = SQLContext.createDataFrame(df_vendas.collect(), getSchema("ft_vendas_tmp"))

    df_vendas2  = df_vendas.join(df_tempo, [df_tempo.id_tempo == df_vendas.id_tempo], "inner").drop(df_tempo.id_tempo).\
                  join(df_cliente, df_cliente.id_cliente == df_vendas.id_cliente, "inner").drop(df_cliente.id_cliente).\
                  join(df_loja, df_loja.id_loja == df_vendas.id_loja, "inner").drop(df_loja.id_loja).\
                  join(df_produto, df_produto.id_produto == df_vendas.id_produto, "inner").drop(df_produto.id_produto)

    df_vendas2.show()
    df_vendasok = df_vendas2.select(col("id_loja"), col("id_produto"), col("id_cliente"), col("id_tempo"), col("qtde_vendida"), col("receita_venda"))
    df_vendasok.show()

    #df_vendas.show()

    #df.show()

    return df_vendasok

def check_ft_vendas(spark : SparkSession):

    df = spark.read.format("JDBC").options(url=SettingProject.getSettings("urldb"),
                    driver=SettingProject.getSettings("driverdb"),
                    user=SettingProject.getSettings("userdb"),
                    password=SettingProject.getSettings("passdb"),
                    dbtable='(select causa, id_loja, id_produto, id_cliente, id_tempo, qtde_vendida, receita_venda From stg_vendas_inconsitencia) tmp').load()

    df.show()

    return df
