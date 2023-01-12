import configparser
import os
import sys

class PrintCustom:
    def ok_extract():
        print("✅ Extração e armazenamento em DateFrame")

    def error_extract(e):
        print("❌ Erro ao realizar extração dos arquivos CSVs ->" + str(e))

    def info(message):
        print("🎲 " + message)

    def info_transformation():
        print("⚙️Realizando Transformações dos dados")

    def ok_transformation():
        print("✅✅ Transformação conluída")

    def info_load():
        print("⏳️Realizando Carregando dos dados Dimensionais")

    def ok_load():
        print("✅✅✅ Carregamento concluído")



class SettingProject:
    def getSettings(psettings):
        Config = configparser.ConfigParser()
        Config.read(os.path.join(sys.path[0], "settings.ini"), encoding='utf-8')
        return Config.get("setproject", psettings)

#print(SettingProject.getSettings("urldb"))