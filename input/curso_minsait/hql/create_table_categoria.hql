CREATE EXTERNAL TABLE IF NOT EXISTS aula_hive.categoria ( 
        id_categoria string, 
        ds_categoria string,
        perc_parceiro string
    )
COMMENT 'tabela de Categoria'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/categoria/'
TBLPROPERTIES ("skip.header.line.count"="1");


CREATE TABLE aula_hive.tbl_categoria ( 
        id_categoria string, 
        ds_categoria string,
        perc_parceiro string
    )
PARTITIONED BY (DT_FOTO STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES ('ORC.COMPRESS'='SNAPPY');

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE
    aula_hive.tbl_categoria
PARTITION(DT_FOTO)
SELECT
id_categoria string,
ds_categoria string,
perc_parceiro string,
'27052023' as DT_FOTO
FROM aula_hive.categoria;