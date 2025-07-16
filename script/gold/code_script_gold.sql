
/******************************************************************************************
* Nom du fichier  : code_script_gold.sql
* Auteur          : Valdes
* Objectif        : Créer les vues de la couche GOLD à partir des tables de la couche SILVER,
*                   pour structurer les données sous forme de dimensions et de faits.
*
* Description     :
* Ce fichier définit trois vues principales dans le schéma `gold`, conformes à un modèle 
* en étoile (star schema), pour permettre des analyses simplifiées en BI ou SQL :
*
* 1. gold.dim_customers :
*    - Vue dimensionnelle contenant les informations clients.
*    - Sources : crm_cust_info, erp_cust_az12, erp_loc_a101.
*    - Nettoie, enrichit et fusionne les données CRM et ERP (pays, genre, date de naissance).
*
* 2. gold.dim_products :
*    - Vue dimensionnelle produits.
*    - Sources : crm_prd_info (données CRM produits), erp_cat_g1v2 (catégories produits).
*    - Ne conserve que les produits actifs (prd_end_dt IS NULL).
*
* 3. gold.fact_sales :
*    - Vue factuelle représentant les ventes.
*    - Source principale : crm_sales_details enrichie des clés clients et produits.
*    - Jointures vers dim_customers et dim_products pour produire un fait complet.
*
* Remarques :
*   - Les clés techniques (customer_key, product_key) sont générées dynamiquement avec 
*     ROW_NUMBER() pour usage BI.
*   - La cohérence des données dépend de la bonne qualité des jointures CRM/ERP.
*
* Usage :
*   - Ces vues peuvent être directement utilisées dans Power BI, Looker, Tableau, etc.
*   - Exemple : SELECT * FROM gold.fact_sales WHERE shipping_date >= '2024-01-01';
*
* Dépendances :
*   - Les vues dépendent entièrement des tables du schéma `silver`.
*   - Assurez-vous que la couche SILVER a été chargée au préalable via les procédures associées.
*
* Dernière mise à jour : 
******************************************************************************************/








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


create or replace view gold.fact_sales as (
select 
sd.sls_ord_num as order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt as order_data,
sd.sls_ship_dt as shipping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price as price
from DWH_RETAIL.SILVER.CRM_SALES_DETAILS sd 
left join gold.dim_products pr
on sd.sls_prd_key=pr.product_number
left join gold.dim_customers cu
on sd.sls_cust_id=cu.customer_id);


    