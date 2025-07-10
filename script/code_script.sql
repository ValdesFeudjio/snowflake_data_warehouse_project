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

