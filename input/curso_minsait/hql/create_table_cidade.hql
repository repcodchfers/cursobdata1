CREATE EXTERNAL TABLE IF NOT EXISTS aula_hive.cidade ( 
        
        id_cidade string, 
        ds_cidade string,
        id_estado string
    )
COMMENT 'tabela de cidade'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/cidade/'
TBLPROPERTIES ("skip.header.line.count"="1");


CREATE TABLE aula_hive.tbl_cidade ( 
        id_cidade string, 
        ds_cidade string,
        id_estado string
    )
PARTITIONED BY (DT_FOTO STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES ('ORC.COMPRESS'='SNAPPY');

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE
    aula_hive.tbl_cidade
PARTITION(DT_FOTO)
SELECT
id_cidade string, 
ds_cidade string,
id_estado string,
${PARTICAO} as DT_FOTO
FROM aula_hive.cidade;