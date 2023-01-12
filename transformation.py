from pyspark.sql import DataFrame, SparkSession
import configparser
from util import SettingProject

def drop_nulls(df : DataFrame):
    return df.dropna()

def transform_ft_vendas(spark: SparkSession):
    tsql = '''               
            select
                id_loja, 
                id_produto, 
                id_cliente,
                id_tempo,
                (random() * 10 + 1)::int as qtde_vendida,
                cast((random() * 10 + 1)::numeric as numeric(12,2)) as receita_venda
            from
                dim_tempo, dim_produto, dim_loja, dim_cliente  
            group by
                id_loja, id_produto, id_cliente, id_tempo
          '''

    df = spark.read.format("jdbc").option("driver", SettingProject.getSettings("driverdb")).option("url",SettingProject.getSettings("urldb")).option("query", tsql).option("user", SettingProject.getSettings("userdb")).option("password", SettingProject.getSettings("passdb")).option("isolationLevel", "NONE").load()
    #df.show()



    return df