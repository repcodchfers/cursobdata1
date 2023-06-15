from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import HiveContext
from pyspark.sql.functions import *
from pyspark.sql import functions as f
import os
import re

spark = SparkSession.builder.master("local[*]")\
    .enableHiveSupport()\
    .getOrCreate()

# Criando dataframes diretamente do Hive

# dim_categoria e dim_produto
df_categoria = spark.sql("select * from aula_hive_stg.tbl_categoria")
df_produto = spark.sql("select * from aula_hive_stg.tbl_produto")
df_subcategoria = spark.sql("select * from aula_hive_stg.tbl_subcategoria")

# dim_filial e dim_localidade
df_cidade = spark.sql("select * from aula_hive_stg.tbl_cidade")
df_cliente = spark.sql("select * from aula_hive_stg.tbl_cliente")
df_estado = spark.sql("select * from aula_hive_stg.tbl_estado")
df_filial = spark.sql("select * from aula_hive_stg.tbl_filial")

# fato_pedido e dim_calendario
df_pedido = spark.sql("select * from aula_hive_stg.tbl_pedido")
df_item_pedido = spark.sql("select * from aula_hive_stg.tbl_item_pedido")

# dim_parceiro
df_parceiro = spark.sql("select * from aula_hive_stg.tbl_parceiro")

# Espaço para tratar e juntar os campos e a criação do modelo dimensional

df_pedido.createOrReplaceTempView('pedido')
df_item_pedido.createOrReplaceTempView('item_pedido')

df_categoria.createOrReplaceTempView('categoria')
df_produto.createOrReplaceTempView('produto')
df_subcategoria.createOrReplaceTempView('subcategoria')

df_cidade.createOrReplaceTempView('cidade')
df_cliente.createOrReplaceTempView('cliente')
df_estado.createOrReplaceTempView('estado')
df_filial.createOrReplaceTempView('filial')

df_parceiro.createOrReplaceTempView('parceiro')

sql = '''
    select 
        p.id_pedido,
        p.dt_pedido,
        p.id_parceiro,
        p.id_cliente,
        p.id_filial,
        p.vr_total_pago,
        ip.id_produto,
        ip.quantidade,
        ip.vr_unitario,
        prd.ds_produto,
        sc.id_subcategoria,
        sc.ds_subcategoria,
        c.id_categoria,
        c.ds_categoria,
        c.perc_parceiro,
        cl.nm_cliente,
        cl.flag_ouro,
        parc.nm_parceiro,
        f.ds_filial,
        f.id_cidade,
        cid.ds_cidade,
        cid.id_estado,
        est.ds_estado        
    from pedido as p 
    inner join item_pedido as ip
    on p.id_pedido == ip.id_pedido
    left join produto as prd on ip.id_produto == prd.id_produto
    left join subcategoria  sc on sc.id_subcategoria == prd.id_subcategoria
    left join categoria  c on c.id_categoria == sc.id_categoria
    left join cliente  cl on cl.id_cliente == p.id_cliente
    left join filial  f on p.id_filial == f.id_filial
    left join cidade  cid on cid.id_cidade == f.id_cidade
    left join estado  est on est.id_estado == cid.id_estado
    left join parceiro  parc on parc.id_parceiro == p.id_parceiro
'''

# Criação da STAGE
df_stage = spark.sql(sql)

# Criação dos Campos Calendario
df_stage = (df_stage
            .withColumn('Ano', year(df_stage.dt_pedido))
            .withColumn('Mes', month(df_stage.dt_pedido))
            .withColumn('Dia', dayofmonth(df_stage.dt_pedido))
            .withColumn('Trimestre', quarter(df_stage.dt_pedido))
           )

# Criação das Chaves do Modelo

df_stage = df_stage.withColumn("DW_PRODUTO", sha2(concat_ws("", df_stage.ds_produto, df_stage.id_produto), 256))
df_stage = df_stage.withColumn("DW_CLIENTE", sha2(concat_ws("", df_stage.nm_cliente, df_stage.flag_ouro, df_stage.id_cliente), 256))
df_stage = df_stage.withColumn("DW_PARCEIRO", sha2(concat_ws("", df_stage.id_parceiro, df_stage.nm_parceiro), 256))
df_stage = df_stage.withColumn("DW_FILIAL", sha2(concat_ws("", df_stage.id_filial, df_stage.ds_filial), 256))
df_stage = df_stage.withColumn("DW_TEMPO", sha2(concat_ws("", df_stage.dt_pedido, df_stage.Ano, df_stage.Mes, df_stage.Dia), 256))
df_stage = df_stage.withColumn("DW_LOCALIDADE", sha2(concat_ws("", df_stage.id_estado, df_stage.id_cidade, df_stage.ds_estado, df_stage.ds_cidade), 256))
df_stage = df_stage.withColumn("DW_CATEGORIA", sha2(concat_ws("", df_stage.id_categoria, df_stage.id_subcategoria, df_stage.ds_categoria, df_stage.ds_subcategoria), 256))

df_stage.createOrReplaceTempView('stage')

#Criando a dimensão Cliente
dim_cliente = spark.sql('''
    SELECT DISTINCT
        DW_CLIENTE,
        id_cliente,
        nm_cliente,
        flag_ouro
    FROM stage    
''')

#Criando a dimensão Produto
dim_produto = spark.sql('''
    SELECT DISTINCT
        DW_PRODUTO,
        id_produto,
        ds_produto
    FROM stage    
''')

#Criando a dimensão Tempo
dim_tempo = spark.sql('''
    SELECT DISTINCT
        DW_TEMPO,
        dt_pedido,
        Ano,
        Mes,
        Dia
    FROM stage    
''')

#Criando a dimensão Localidade
dim_localidade = spark.sql('''
    SELECT DISTINCT
        DW_LOCALIDADE,
        id_estado,
        id_cidade,
        ds_estado,
        ds_cidade
    FROM stage    
''')

#Criando a dimensão filial
dim_filial = spark.sql('''
    SELECT DISTINCT
        DW_FILIAL,
        id_filial,
        ds_filial
    FROM stage    
''')

#Criando a dimensão Parceiro
dim_parceiro = spark.sql('''
    SELECT DISTINCT
        DW_PARCEIRO,
        id_parceiro,
        nm_parceiro
    FROM stage    
''')

#Criando a dimensão Categoria
dim_categoria = spark.sql('''
    SELECT DISTINCT
        DW_CATEGORIA,
        id_categoria,
        id_subcategoria,
        ds_categoria,
        ds_subcategoria
    FROM stage    
''')

#Criando a Fato Pedidios
ft_pedidos = spark.sql('''
    SELECT 
        DW_PARCEIRO,
        DW_CLIENTE,
        DW_FILIAL,
        DW_LOCALIDADE,
        DW_TEMPO,
        sum(vr_total_pedido) as vl_total
    FROM stage
    group by 
        DW_PARCEIRO,
        DW_CLIENTE,
        DW_FILIAL,
        DW_LOCALIDADE,
        DW_TEMPO
''')

# função para salvar os dados
def salvar_df(df, file):
    output = "/input/curso_minsait/gold/" + file
    erase = "hdfs dfs -rm " + output + "/*"
    rename = "hdfs dfs -get /datalake/gold/"+file+"/part-* /input/curso_minsait/gold/"+file+".csv"
    print(rename)
    
    
    df.coalesce(1).write\
        .format("csv")\
        .option("header", True)\
        .option("delimiter", ";")\
        .mode("overwrite")\
        .save("/datalake/gold/"+file+"/")

    os.system(erase)
    os.system(rename)

salvar_df(ft_pedidos, 'ft_pedidos')
salvar_df(dim_cliente, 'dim_cliente')
salvar_df(dim_tempo, 'dim_tempo')
salvar_df(dim_localidade, 'dim_localidade')
salvar_df(dim_produto, 'dim_produto')
salvar_df(dim_parceiro, 'dim_parceiro')
salvar_df(dim_categoria, 'dim_categoria')