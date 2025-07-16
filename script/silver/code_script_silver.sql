/******************************************************************************************
* Nom du fichier  : load_silver_procedures.sql
* Auteur          : Valdes
* Objectif        : Automatiser le traitement et le chargement des données de la couche 
*                   BRONZE vers la couche SILVER dans Snowflake à l’aide de procédures SQL.
*
* Description     :
* Ce fichier contient 6 procédures stockées, chacune dédiée au traitement et à l’insertion
* des données dans une table de la couche SILVER. Les transformations incluent :
*   - Nettoyage des valeurs (TRIM, COALESCE, CASE…)
*   - Normalisation des formats (genre, statut marital, pays…)
*   - Filtrage et enrichissement (ROW_NUMBER, LEAD…)
*   - Conversion de dates (format YYYYMMDD → DATE)
*
* Les procédures concernent les tables suivantes :
*   - silver.crm_cust_info
*   - silver.crm_prd_info
*   - silver.crm_sales_details
*   - silver.erp_cust_az12
*   - silver.erp_loc_a101
*   - silver.erp_cat_g1v2
*
* Une 7e procédure, appelée SP_LOAD_ALL_SILVER(), appelle l’ensemble des 6 procédures
* pour faciliter un rechargement complet et centralisé de la couche SILVER.
*
* Usage :
*   - Pour exécuter une procédure spécifique : 
*       CALL silver.SP_LOAD_CRM_CUST_INFO();
*
*   - Pour tout recharger d’un coup :
*       CALL silver.SP_LOAD_ALL_SILVER();
*
* Dépendances :
*   - Les tables BRONZE doivent être préalablement alimentées.
*   - Les tables SILVER doivent exister dans le schéma `silver`.
*
* Dernière mise à jour 
******************************************************************************************/






CALL silver.SP_LOAD_ALL_SILVER(); -- j'execute la procedure globale de chargement des données dans les tables silver

--- cretation des tables de la zone silver
-- cas de  la table CRM

create table if not exists silver.crm_cust_info(
    cst_id int,
    cst_key varchar(25),
    cst_firstname varchar(30),
    cst_lastname varchar(30),
    cst_marital_status varchar(30),
    cst_gndr varchar(30),
    cst_create_date date
);

--drop table silver.crm_prd_info;
create table if not exists silver.crm_prd_info(
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

create table if not exists silver.crm_sales_details(
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


create table if not exists silver.erp_cust_az12(
    
    cid varchar(50),
    bdate date,
    gen varchar(20)
);


create table if not exists silver.erp_loc_a101(
    
    cid varchar(50),
    cntry varchar(20)
);



create table if not exists silver.erp_cat_g1v2(
    
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


CREATE OR REPLACE PROCEDURE silver.SP_LOAD_CRM_CUST_INFO()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  TRUNCATE TABLE silver.crm_cust_info;

  INSERT INTO silver.crm_cust_info(
    cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date
  )
  SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname),
    TRIM(cst_lastname),
    CASE
      WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
      WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
      ELSE 'n/a'
    END,
    CASE
      WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
      WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
      ELSE 'n/a'
    END,
    cst_create_date
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
  ) t
  WHERE flag_last = 1;

  RETURN 'crm_cust_info loaded';
END;
$$;




-- Cas de la table crm_prd_info

/*
Les traitements de bases de la table crm_prd_info sont detaillés dans la feuille silver_test
Nous donnons tous les détails sur chaque variable
*/

CREATE OR REPLACE PROCEDURE silver.SP_LOAD_CRM_PRD_INFO()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  TRUNCATE TABLE silver.crm_prd_info;

  INSERT INTO silver.crm_prd_info (
    prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt
  )
  SELECT
    prd_id,
    REPLACE(SUBSTR(prd_key,1,5),'-','_'),
    SUBSTR(prd_key, 7, LENGTH(prd_key)),
    prd_nm,
    COALESCE(prd_cost, 0),
    CASE
      WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
      WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
      WHEN UPPER(TRIM(prd_line))='S' THEN 'Other Sales'
      WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
      ELSE 'n/a'
    END,
    prd_start_dt,
    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1
  FROM bronze.crm_prd_info;

  RETURN 'crm_prd_info loaded';
END;
$$;






-- Cas de la table crm_sales_details

/*
Les traitements de bases de la table crm_prd_info sont detaillés dans la feuille silver_test
Nous donnons tous les détails sur chaque variable
*/

CREATE OR REPLACE PROCEDURE silver.SP_LOAD_CRM_SALES_DETAILS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  TRUNCATE TABLE silver.crm_sales_details;

  INSERT INTO silver.crm_sales_details (
    sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
  )
  SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt=0 OR LENGTH(sls_order_dt)!=8 THEN NULL ELSE TO_DATE(TO_VARCHAR(sls_order_dt), 'yyyymmdd') END,
    CASE WHEN sls_ship_dt=0 OR LENGTH(sls_ship_dt)!=8 THEN NULL ELSE TO_DATE(TO_VARCHAR(sls_ship_dt), 'yyyymmdd') END,
    CASE WHEN sls_due_dt=0 OR LENGTH(sls_due_dt)!=8 THEN NULL ELSE TO_DATE(TO_VARCHAR(sls_due_dt), 'yyyymmdd') END,
    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
         THEN sls_quantity * ABS(sls_price)
         ELSE sls_sales END,
    sls_quantity,
    CASE WHEN sls_price IS NULL OR sls_price <= 0
         THEN ROUND(sls_sales / NULLIF(sls_quantity, 0), 0)
         ELSE ROUND(sls_price, 0) END
  FROM bronze.crm_sales_details;

  RETURN 'crm_sales_details loaded';
END;
$$;






   -----------------------------------------------------------------
  --------------- TRAITEMENT DES DONNEES SILVER ERP ---------------
----------------------------------------------------------------

/* Cas de la table erp_cust_az12 de silver*/


CREATE OR REPLACE PROCEDURE silver.SP_LOAD_ERP_CUST_AZ12()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  TRUNCATE TABLE silver.erp_cust_az12;

  INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
  SELECT
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTR(cid,4,LENGTH(cid)-3) ELSE cid END,
    CASE WHEN bdate > CURRENT_DATE() THEN NULL ELSE bdate END,
    CASE
      WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
      WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
      ELSE 'n/a'
    END
  FROM bronze.erp_cust_az12;

  RETURN 'erp_cust_az12 loaded';
END;
$$;



/* Cas de la table erp_loc_101 de silver*/




CREATE OR REPLACE PROCEDURE silver.SP_LOAD_ERP_LOC_A101()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  TRUNCATE TABLE silver.erp_loc_a101;

  INSERT INTO silver.erp_loc_a101 (cid, cntry)
  SELECT
    REPLACE(cid, '-'),
    CASE
      WHEN TRIM(cntry) = 'DE' THEN 'Germany'
      WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
      WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
      ELSE TRIM(cntry)
    END
  FROM bronze.erp_loc_a101;

  RETURN 'erp_loc_a101 loaded';
END;
$$;




/* Procedure globale pour l'execution des procedure precedentes*/


CREATE OR REPLACE PROCEDURE silver.SP_LOAD_ERP_CAT_G1V2()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  TRUNCATE TABLE silver.erp_cat_g1v2;

  INSERT INTO silver.erp_cat_g1v2 (id, cat, subcat, maintenance)
  SELECT id, cat, subcat, maintenance
  FROM bronze.erp_cat_g1v2;

  RETURN 'erp_cat_g1v2 loaded';
END;
$$;





CREATE OR REPLACE PROCEDURE silver.SP_LOAD_ALL_SILVER()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  CALL silver.SP_LOAD_CRM_CUST_INFO();
  CALL silver.SP_LOAD_CRM_PRD_INFO();
  CALL silver.SP_LOAD_CRM_SALES_DETAILS();
  CALL silver.SP_LOAD_ERP_CUST_AZ12();
  CALL silver.SP_LOAD_ERP_LOC_A101();
  CALL silver.SP_LOAD_ERP_CAT_G1V2();

  RETURN 'Toutes les tables silver ont été chargées.';
END;
$$;







