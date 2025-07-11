/* 
-------------------------------------------------
 -Creation de la base de données et des schemas- 
-------------------------------------------------

Explication :
    le script ci dessous permet de creer la base de données et DWH_RETAIL en verifiant en amont si elle n'existe pas déjà pour éviter de l'écraser.
    Additionnelement, le script permet de creer nos 03 schemas qui vont permettre de recvoir les 03 types de données dans notre process de traitement (bonze, silver et gold).
    

*/



create if not exists database DWH_RETAIL;
use database DWH_RETAIL;

create or replace schema bronze;
create or replace schema silver;
create or replace schema gold;



--- cretation des tables de la zone bronze
-- cas de  la table CRM

create table bronze.crm_cust_info(
    cst_id int,
    cst_key varchar(25),
    cst_firstname varchar(30),
    cst_lastname varchar(30),
    cst_marital_status varchar(30),
    cst_gndr varchar(30),
    cst_create_date date
);






create table bronze.crm_prd_info(
    prd_id int,
    prd_key varchar(25),
    prd_nm varchar(25),
    prd_cost int,
    prd_line varchar(25),
    prd_start_dt date,
    prd_end_dt date
);


create table bronze.crm_sales_details(
    sls_ord_num varchar(25),
    sls_prd_key varchar(25),
    sls_cust_id int,
    sls_order_dt int,
    sls_ship_dt int,
    sls_due_dt int,
    sls_sales int,
    sls_quantity int,
    sls_price int
);

-- cas de  la table ERP


create table bronze.erp_cust_az12(
    
    cid varchar(50),
    bdate date,
    gen varchar(20)
);


create table bronze.erp_loc_a101(
    
    cid varchar(50),
    cntry varchar(20)
);



create table bronze.erp_cat_g1v2(
    
id varchar(50),
cat varchar(50),
subcat varchar(50),
maintenance varchar(50)
);



/*
-- je fais une intégration snowflake apres avoir crée le role i am sur AWS
*/

CREATE OR REPLACE STORAGE INTEGRATION s3_int_retail
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::115181208395:role/snowflake--retail-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://retail-bucket-dwh/');


desc integration s3_int_retail;


create or replace file format retail_format
FIELD_DELIMITER = ','; 



CREATE OR REPLACE STAGE stage_retail_crm
  STORAGE_INTEGRATION = s3_int_retail
  URL = 's3://retail-bucket-dwh/CRM/'
  FILE_FORMAT = (TYPE = 'CSV');


CREATE OR REPLACE STAGE stage_retail_erp
  STORAGE_INTEGRATION = s3_int_retail
  URL = 's3://retail-bucket-dwh/CRM/'
  FILE_FORMAT = (TYPE = 'CSV');


LIST @stage_retail_crm;
LIST @stage_retail_erp;



