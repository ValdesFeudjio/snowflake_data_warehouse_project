
-- Dimension customers
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





    -- Dimension products
create or replace view gold.dim_products as (
    select 
    row_number() over (order by pn.prd_start_dt, pn.prd_key) as product_key,
    pn.prd_id as product_id,
    pn.prd_key as product_number,
    pn.prd_nm as product_name,
    pn.cat_id as category_id,
    pc.cat as category,
    pc.subcat as subcategory,
    pc.maintenance,
    pn.prd_cost as cost,
    pn.prd_line as product_line,
    pn.prd_start_dt as start_date
    from silver.crm_prd_info pn 
    left join silver.erp_cat_g1v2 pc
    on pn.cat_id=pc.id
    where pn.prd_end_dt is null -- Filter out all historical data to keep only the current products
);






-- fact sales view
    