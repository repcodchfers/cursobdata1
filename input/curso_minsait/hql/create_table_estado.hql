
CREATE EXTERNAL TABLE IF NOT EXISTS aula_hive.estado ( 
        
        id_estado string, 
        ds_estado string
        
    )
COMMENT 'tabela de estado'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/estado/'
TBLPROPERTIES ("skip.header.line.count"="1");


CREATE TABLE aula_hive.tbl_estado ( 
        id_estado string, 
        ds_estado string
    )
PARTITIONED BY (DT_FOTO STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES ('ORC.COMPRESS'='SNAPPY');

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE
    aula_hive.tbl_estado
PARTITION(DT_FOTO)
SELECT
id_estado string, 
ds_estado string,
${PARTICAO} as DT_FOTO
FROM aula_hive.estado;