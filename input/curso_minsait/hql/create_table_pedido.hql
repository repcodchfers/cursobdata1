
CREATE EXTERNAL TABLE IF NOT EXISTS aula_hive.pedido ( 
        
        id_pedido string, 
        dt_pedido string,
        id_parceiro string,
        id_cliente string,
        id_filial string,
        vr_total_pago string
    )
COMMENT 'tabela de pedido'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/datalake/raw/pedido/'
TBLPROPERTIES ("skip.header.line.count"="1");


CREATE TABLE aula_hive.tbl_pedido ( 
        id_pedido string, 
        dt_pedido string,
        id_parceiro string,
        id_cliente string,
        id_filial string,
        vr_total_pago string
    )
PARTITIONED BY (DT_FOTO STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
TBLPROPERTIES ('ORC.COMPRESS'='SNAPPY');

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE
    aula_hive.tbl_pedido
PARTITION(DT_FOTO)
SELECT
id_pedido string, 
dt_pedido string,
id_parceiro string,
id_cliente string,
id_filial string,
vr_total_pago string,
${PARTICAO} as DT_FOTO
FROM aula_hive.pedido;