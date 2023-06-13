#!bin/bash
DATE="$(date --date="-0 day" "+%Y%m%d")"
#TABLES=("cidade" "estado" "filial" "parceiro" "cliente" "subcategoria" "categoria" "item_pedido" "produto")
#TABLES=("estado" "filial" "parceiro" "cliente" "subcategoria" "item_pedido" "produto")
#TABLES=("pedido" "item_pedido" "produto")
TABLES=("pedido" )
TABLE_CLIENTE="TBL_CLIENTE"
PARTICAO="$(date --date="-0 day" "+%Y%m%d")"