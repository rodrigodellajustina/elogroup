
# Projeto Elogroup - ETL

Projeto avaliação de performance de candidato a vaga para engenheiro de dados.




[![MIT License](https://img.shields.io/badge/License-MIT-green.svg)](https://choosealicense.com/licenses/mit/)
[![PyPI Latest Release](https://img.shields.io/pypi/v/pandas.svg)](https://pypi.org/project/pandas/)
![GitHub Pipenv locked Python version](https://img.shields.io/github/pipenv/locked/python-version/2/2?label=Python%203.8&style=plastic)
## Autores

- [@rodrigodellajustina](https://github.com/rodrigodellajustina/)


# Configuração

## settings.ini

[setproject]
```
urldb    : jdbc:postgresql://localhost:5432/nomedobanco
userdb   : postgres
passdb   : senha
driverdb : org.postgresql.Driver
hdfs     : <repositório hdfs>
```

# Dependências
```
pip install -r requirements.txt
```

# Executando Projeto

#### Extraíndo/Transformando/Carregando Dimensões para Modelos de Dados

```
>python main.py dim
```
```
� Extraíndo informação DM_Tempo.csv
� Extraíndo informação DM_Cliente.csv
� Extraíndo informação DM_Loja.csv
� Extraíndo informação DM_Produto.csv
✅ Extração e armazenamento em DateFrame
⚙️Realizando Transformações dos dados
✅✅ Transformação conluída
⏳️Realizando Carregando dos dados Dimensionais
� Armazenando dados em dim_tempo
� Armazenando dados em dim_cliente
� Armazenando dados em dim_loja
� Armazenando dados em dim_produto
✅✅✅ Carregamento concluído
```


#### Extraíndo/Transformando/Carregando Entidades Fatos
```
>python main.py fat
```

```
⚙️Realizando Transformações dos dados
✅✅ Transformação conluída
⏳️Realizando Carregando dos dados Dimensionais
� Armazenando dados em ft_vendas
✅✅✅ Carregamento concluído
```