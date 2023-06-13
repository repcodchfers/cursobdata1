

CREATE EXTERNAL TABLE IF NOT EXISTS aula_hive.item_pedido ( 
        
        id_pedido string, 
        id_produto string,
        quantidade string,
        vr_unitario string
    )
COMMENT 'tabela de item_pedido'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/item_pedido/'
TBLPROPERTIES ("skip.header.line.count"="1");


CREATE TABLE aula_hive.tbl_item_pedido ( 
        id_pedido string, 
        id_produto string,
        quantidade string,
        vr_unitario string
    )
PARTITIONED BY (DT_FOTO STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES ('ORC.COMPRESS'='SNAPPY');

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE
    aula_hive.tbl_item_pedido
PARTITION(DT_FOTO)
SELECT
id_pedido string, 
id_produto string,
quantidade string,
vr_unitario string,
'06062023' as DT_FOTO
FROM aula_hive.item_pedido;