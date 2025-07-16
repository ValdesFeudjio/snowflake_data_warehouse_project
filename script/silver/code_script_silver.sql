

--- cretation des tables de la zone silver
-- cas de  la table CRM

create table silver.crm_cust_info(
    cst_id int,
    cst_key varchar(25),
    cst_firstname varchar(30),
    cst_lastname varchar(30),
    cst_marital_status varchar(30),
    cst_gndr varchar(30),
    cst_create_date date
);

--drop table silver.crm_prd_info;
create table silver.crm_prd_info(
    prd_id int,
    cat_id varchar(50),
    prd_key varchar(50),
    prd_nm varchar(50),
    prd_cost int,
    prd_line varchar(50),
    prd_start_dt date,
    prd_end_dt date
);

--drop table silver.crm_sales_details;

create table silver.crm_sales_details(
    sls_ord_num varchar(25),
    sls_prd_key varchar(25),
    sls_cust_id int,
    sls_order_dt date,
    sls_ship_dt date,
    sls_due_dt date,
    sls_sales int,
    sls_quantity int,
    sls_price int
);



-- cas de  la table ERP


create table silver.erp_cust_az12(
    
    cid varchar(50),
    bdate date,
    gen varchar(20)
);


create table silver.erp_loc_a101(
    
    cid varchar(50),
    cntry varchar(20)
);



create table silver.erp_cat_g1v2(
    
id varchar(50),
cat varchar(50),
subcat varchar(50),
maintenance varchar(50)
);






   -----------------------------------------------------------------
  --------------- TRAITEMENT DES DONNEES SILVER CRM ---------------
----------------------------------------------------------------

-- Cas de la table crm_cust_info

/*
Les traitements de bases de la table crm_cust_info sont detaillés dans la feuille silver_test
Nous donnons tous les détails sur chaque variable
*/

truncate table silver.crm_cust_info;

insert into silver.crm_cust_info(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)

select cst_id,
       cst_key,
       trim(cst_firstname) as cst_firstname,
       trim(cst_lastname) as cst_lastname,
        case
            when upper(trim(cst_marital_status))='S' then 'Single'
            when upper(trim(cst_marital_status))='M' then 'Married'
            else 'n/a'
       end cst_marital_status,  -- normalise marital status values to readable format
       case
            when upper(trim(cst_gndr))='F' then 'Female'
            when upper(trim(cst_gndr))='M' then 'Male'
            else 'n/a'
       end cst_gndr,  -- Normalize gender values to readable format
       cst_create_date
from (
select *,
       row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info
where cst_id is not null) t
where flag_last=1;


-- Cas de la table crm_prd_info

/*
Les traitements de bases de la table crm_prd_info sont detaillés dans la feuille silver_test
Nous donnons tous les détails sur chaque variable
*/


insert into silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)

select
prd_id,
replace (substr(prd_key,1,5),'-','_') as cat_id, -- derived new columns
substr(prd_key, 7, length(prd_key)) as prd_key,
prd_nm,
coalesce(prd_cost,0) as prd_cost,
case
    when upper(trim(prd_line))='M' then 'Mountain'
    when upper(trim(prd_line))='R' then 'Road'
    when upper(trim(prd_line))='S' then 'Other Sales'
    when upper(trim(prd_line))='T' then 'Touring'
    else 'n/a'
end as prd_line,
prd_start_dt,
lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as prd_end_dt
from bronze.crm_prd_info;



-- Cas de la table crm_sales_details

/*
Les traitements de bases de la table crm_prd_info sont detaillés dans la feuille silver_test
Nous donnons tous les détails sur chaque variable
*/



insert into silver.crm_sales_details(
    sls_ord_num ,
    sls_prd_key ,
    sls_cust_id ,
    sls_order_dt ,
    sls_ship_dt ,
    sls_due_dt ,
    sls_sales ,
    sls_quantity ,
    sls_price 
)

SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,

case
    when sls_order_dt=0 or length(sls_order_dt) != 8 then null 
    else to_date(to_varchar(sls_order_dt), 'yyyymmdd') 
end as sls_order_dt,

case
    when sls_ship_dt=0 or length(sls_ship_dt) != 8 then null 
    else to_date(to_varchar(sls_ship_dt), 'yyyymmdd') 
end as sls_ship_dt,

case
    when sls_due_dt=0 or length(sls_due_dt) != 8 then null 
    else to_date(to_varchar(sls_due_dt), 'yyyymmdd') 
end as sls_due_dt,

case
    when sls_sales is null or sls_sales <= 0 or sls_sales!=sls_quantity* abs(sls_price) then sls_quantity*abs(sls_price)
    else sls_sales
end sls_sales,

sls_quantity,

case 
    when sls_price is null or sls_price<=0 then round(sls_sales/nullif(sls_quantity,0),0)
    else round(sls_price,0)
end as sls_price 

FROM BRONZE.CRM_SALES_DETAILS;




   -----------------------------------------------------------------
  --------------- TRAITEMENT DES DONNEES SILVER ERP ---------------
----------------------------------------------------------------

insert into silver.erp_cust_az12(
    
    cid ,
    bdate ,
    gen
)

select 
case
    when cid like 'NAS%' then substr(cid,4,length(cid)-3)
    else cid
end cid,

case 
    when bdate>current_date() then null 
    else bdate
end as bdate,
case
    when upper(trim(gen)) in ('F','FEMALE') then 'Female'
    when upper(trim(gen)) in ('M','MALE') then 'Male'
    else 'n/a'
end as gen
from bronze.erp_cust_az12;