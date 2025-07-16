
-- Dimension customer
create or replace view gold.dim_customers as (
select     row_number() over (order by cst_id) as customer_key,
           ci.cst_id as customer_id,
           ci.cst_key as customer_number ,
           ci.cst_firstname as first_name,
           ci.cst_lastname as last_name,
           el.cntry as country,
           ci.cst_marital_status as marital_status,
           case 
                when ci.cst_gndr !='n/a' then ci.cst_gndr -- crm is the best information source for gender
                else coalesce(ca.gen,'n/a')
           end as gender,
           ca.bdate as birthdate,
           ci.cst_create_date as create_date   
           
    from silver.crm_cust_info ci 
    left join silver.erp_cust_az12 ca on ci.cst_key=ca.cid 
    left join SILVER.ERP_LOC_A101 el on ci.cst_key=el.cid);