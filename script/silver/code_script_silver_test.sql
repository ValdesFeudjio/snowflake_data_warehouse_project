/*
Feuille de test
*/
-- Cas de la table crm_cust_info

--check for nulls or duplicates in primary key

/*
si les valeur sont duppliquées, nous allons garder uniquement les infos les plus recentes par client
*/
select cst_id,
       count(*) as nombre
from bronze.crm_cust_info
group by cst_id
having nombre>1  or cst_id is null ;


select *
from bronze.crm_cust_info
where cst_id=29449; 


select *
from (
select *,
       row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info)
where flag_last=1;


-- verification du formatage des noms et des prenoms

/*
Nous allons utiliser trim pour mettre en forme les noms et les prenom
*/

select cst_gndr
from bronze.crm_cust_info
Where cst_gndr != trim(cst_gndr); -- ici trim permet de supprimer les espaces en début et en fin des expressions




/*
On regarde la colonne status marital et genre 
*/

select distinct cst_gndr
from bronze.crm_cust_info;

select distinct cst_marital_status
from bronze.crm_cust_info;



/*
on regarde la table qui donne les informations sur le produit
*/

-- valeur dupliquée pour id du produit
select prd_id,
       count(*) as nombre
from bronze.crm_prd_info
group by prd_id
having nombre>1  or prd_id is null ;

-- valeur dupliquée pour key du produit
select prd_key,
       count(*) as nombre
from bronze.crm_prd_info
group by prd_key
having nombre>1  or prd_key is null ;

--

select
prd_id,
prd_key,
replace (substr(prd_key,1,5),'-','_') as cat_id,
substr(prd_key, 7, length(prd_key)) as prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info;



select prd_nm
from bronze.crm_prd_info
where prd_nm != trim(prd_nm);


select prd_cost
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null; 


select distinct prd_line
from bronze.crm_prd_info;

-- traitment des dates 
select *
from bronze.crm_prd_info
where prd_end_dt<prd_start_dt;



select
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as prd_end_dt_test
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R','AC-HE-HL-U509');








SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
FROM BRONZE.CRM_SALES_DETAILS
where sls_order_dt<=0;


select
nullif(sls_due_dt,0) sls_due_dt
FROM BRONZE.CRM_SALES_DETAILS
where sls_due_dt<=0 
or length(sls_due_dt) != 8
or sls_due_dt>20500101
or sls_due_dt<19000101;



select 
sls_sales as old_sls_sales,
sls_quantity,
sls_price as old_sls_price,
case
    when sls_sales is null or sls_sales <= 0 or sls_sales!=sls_quantity* abs(sls_price) then sls_quantity*abs(sls_price)
    else sls_sales
end sls_sales,

case 
    when sls_price is null or sls_price<=0 then sls_sales/nullif(sls_quantity,0)
    else sls_price
end as sls_price 
    
from BRONZE.CRM_SALES_DETAILS
where sls_sales!= sls_quantity*sls_price
or sls_sales is null
or sls_quantity is null 
or sls_price is null
or sls_sales<=0
or sls_quantity<=0
or sls_price<=0;





-- Cas de ERP

/*
On commence par la table erp_cust_az12 et on s'assure de la bonne qualité des données dans la table 
*/


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



select distinct gen,
case
    when upper(trim(gen)) in ('F','FEMALE') then 'Female'
    when upper(trim(gen)) in ('M','MALE') then 'Male'
    else 'n/a'
end as gen
from bronze.erp_cust_az12;






















