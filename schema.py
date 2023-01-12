from pyspark.sql.types import  StructType, StringType, IntegerType, DateType, StructField
import json

def setSchema():
    # Generate Schema Entity
    schemaDimTempo = StructType([
                StructField("id_tempo", IntegerType()),
                StructField("ano", IntegerType()),
                StructField("mes", IntegerType()),
                StructField("dia", IntegerType()),
                StructField("dt", DateType())
            ])

    with open("schema/dim_tempo.json", "w") as f:
        json.dump(schemaDimTempo.jsonValue(), f)

    schemaDimProduto = StructType([
        StructField("id_produto", IntegerType()),
        StructField("cod_produto", IntegerType()),
        StructField("nm_produto", StringType()),
        StructField("secao", StringType()),
        StructField("grupo", StringType()),
        StructField("subgrupo", StringType()),
    ])

    with open("schema/dim_produto.json", "w") as f:
        json.dump(schemaDimProduto.jsonValue(), f)

    schemaDimLoja = StructType([
        StructField("id_loja", IntegerType()),
        StructField("cod_loja", IntegerType()),
        StructField("nm_loja", StringType())
    ])

    with open("schema/dim_loja.json", "w") as f:
        json.dump(schemaDimLoja.jsonValue(), f)

    schemaDimCliente = StructType([
        StructField("id_cliente", IntegerType()),
        StructField("estado_civil", StringType()),
        StructField("sexo", StringType()),
        StructField("bairro", StringType()),
    ])

    with open("schema/dim_cliente.json", "w") as f:
        json.dump(schemaDimCliente.jsonValue(), f)


def getSchema(schema : str):

    with open("schema/{0}.json".format(schema)) as f:
        new_schema = StructType.fromJson(json.load(f))
        #print(new_schema)

    return new_schema

def getTable(file : str):
    listTableFile = [["DM_Cliente.csv", "dim_cliente"],
                     ["DM_Loja.csv", "dim_loja"],
                     ["DM_Produto.csv", "dim_produto"],
                     ["DM_Tempo.csv", "dim_tempo"]
                    ]

    return list(filter(lambda x: x[0]==file, listTableFile))[0][1]


#setSchema()
#print(getTable("DM_Tempo.csv"))
#print(getSchema("dim_tempo"))