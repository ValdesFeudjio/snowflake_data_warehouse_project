
CALL DWH_RETAIL.bronze.SP_LOAD_ALL_DATA(); -- run the bronze layer
CALL DWH_RETAIL.silver.SP_LOAD_ALL_SILVER(); -- run the silver layer
CALL DWH_RETAIL.silver.SP_LOAD_ALL_GOLD(); -- run the gold layer