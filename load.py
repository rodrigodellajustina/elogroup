from pyspark.sql import DataFrame
from util import SettingProject
from util import PrintCustom
# substituir por delta.tables  https://docs.delta.io/

def load(type: str, df : DataFrame, table: str):
    PrintCustom.info("Armazenando dados em " + table)
    if type=="JDBC":
        df.write.format("JDBC").mode("append").\
            options(url=SettingProject.getSettings("urldb"),
                    driver=SettingProject.getSettings("driverdb"),
                    user=SettingProject.getSettings("userdb"),
                    password=SettingProject.getSettings("passdb"),
                    dbtable=table).save()