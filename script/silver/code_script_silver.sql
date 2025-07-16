

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


create table silver.crm_sales_details(
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
replace (substr(prd_key,1,5),'-','_') as cat_id,
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

--drop stage stage_retail_erp;

CREATE OR REPLACE STAGE stage_retail_erp
  STORAGE_INTEGRATION = s3_int_retail
  URL = 's3://retail-bucket-dwh/ERP/'
  FILE_FORMAT = (TYPE = 'CSV');


LIST @stage_retail_crm;
LIST @stage_retail_erp;





-- je charge les fichiers du stage dans les TABLES

-- cas de crm


-- infos sur le customer
CREATE OR REPLACE PROCEDURE DWH_RETAIL.BRONZE.LOAD_CRM_CUST_INFO()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    COPY INTO DWH_RETAIL.BRONZE.CRM_CUST_INFO
    FROM @DWH_RETAIL.BRONZE.STAGE_RETAIL_CRM/cust_info.csv
    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);
    
    RETURN 'Data loaded successfully';
END;
$$;


-- infos sur le produit
CREATE OR REPLACE PROCEDURE DWH_RETAIL.BRONZE.LOAD_CRM_PRD_INFO()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    COPY INTO DWH_RETAIL.BRONZE.CRM_PRD_INFO
    FROM @DWH_RETAIL.BRONZE.STAGE_RETAIL_CRM/prd_info.csv
    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);
    
    RETURN 'Data loaded successfully';
END;
$$;


-- details des ventes
CREATE OR REPLACE PROCEDURE DWH_RETAIL.BRONZE.LOAD_CRM_SALES_DETAILS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    COPY INTO DWH_RETAIL.BRONZE.CRM_SALES_DETAILS
    FROM @DWH_RETAIL.BRONZE.STAGE_RETAIL_CRM/sales_details.csv
    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);
    
    RETURN 'Data loaded successfully';
END;
$$;




-- cas de erp

CREATE OR REPLACE PROCEDURE DWH_RETAIL.BRONZE.LOAD_ERP_CAT_G1V2()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    COPY INTO DWH_RETAIL.BRONZE.ERP_CAT_G1V2
    FROM @DWH_RETAIL.BRONZE.STAGE_RETAIL_ERP/PX_CAT_G1V2.csv
    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);
    
    RETURN 'Data loaded successfully';
END;
$$;


CREATE OR REPLACE PROCEDURE DWH_RETAIL.BRONZE.LOAD_ERP_CUST_AZ12()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    COPY INTO DWH_RETAIL.BRONZE.ERP_CUST_AZ12
    FROM @DWH_RETAIL.BRONZE.STAGE_RETAIL_ERP/CUST_AZ12.csv
    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);
    
    RETURN 'Data loaded successfully';
END;
$$;



CREATE OR REPLACE PROCEDURE DWH_RETAIL.BRONZE.LOAD_ERP_LOC_A101()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    COPY INTO DWH_RETAIL.BRONZE.ERP_LOC_A101
    FROM @DWH_RETAIL.BRONZE.STAGE_RETAIL_ERP/LOC_A101.csv
    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);
    
    RETURN 'Data loaded successfully';
END;
$$;




-- Procedure globlale de l'ingestion des 6 tables 

-- Étape 1 : Création de l'intégration email pour autoriser l'envoi de mails
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE SYSADMIN;

CREATE NOTIFICATION INTEGRATION email_alerts
  TYPE = EMAIL
  ENABLED = TRUE
  ALLOWED_RECIPIENTS = ('valdesfeudjio@gmail.com');


-- Étape 2 : Procédure qui envoie un email en cas de succès de l'ingestion
CREATE OR REPLACE PROCEDURE DWH_RETAIL.BRONZE.SP_NOTIFY_LOAD_SUCCESS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  CALL SYSTEM$SEND_EMAIL(
    'email_alerts',  -- Nom de l'intégration définie à l'étape 1
    'valdesfeudjio@gmail.com',
    '✅ Succès : Chargement DWH terminé',
    'Tous les fichiers CRM et ERP ont été chargés avec succès.'
  );

  RETURN 'Notification de succès envoyée';
END;
$$;


-- Étape 3 : Procédure qui envoie un email si une erreur survient pendant l'ingestion
CREATE OR REPLACE PROCEDURE DWH_RETAIL.BRONZE.SP_NOTIFY_LOAD_FAILURE(error_message STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  CALL SYSTEM$SEND_EMAIL(
    'email_alerts',
    'valdesfeudjio@gmail.com',
    '❌ Échec : Problème de chargement DWH',
    'Le chargement a échoué avec l’erreur suivante : ' || error_message
  );

  RETURN 'Notification d’échec envoyée';
END;
$$;




-- Étape 4 : Procédure globale qui appelle les 6 procédures d'ingestion
-- et déclenche l'envoi du mail de succès ou d'échec
CREATE OR REPLACE PROCEDURE DWH_RETAIL.BRONZE.SP_LOAD_ALL_DATA()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  err_msg STRING;
BEGIN
  BEGIN
    -- CRM
    CALL DWH_RETAIL.BRONZE.LOAD_CRM_CUST_INFO();
    CALL DWH_RETAIL.BRONZE.LOAD_CRM_PRD_INFO();
    CALL DWH_RETAIL.BRONZE.LOAD_CRM_SALES_DETAILS();

    -- ERP
    CALL DWH_RETAIL.BRONZE.LOAD_ERP_CAT_G1V2();
    CALL DWH_RETAIL.BRONZE.LOAD_ERP_CUST_AZ12();
    CALL DWH_RETAIL.BRONZE.LOAD_ERP_LOC_A101();

    -- Success notification
    CALL DWH_RETAIL.BRONZE.SP_NOTIFY_LOAD_SUCCESS();

    RETURN 'All loads completed successfully.';

  EXCEPTION
    WHEN OTHER THEN
      SET err_msg = SQLSTATE || ': ' || SQLERRM;
      CALL DWH_RETAIL.BRONZE.SP_NOTIFY_LOAD_FAILURE(:err_msg);
      RETURN 'Error during load: ' || :err_msg;
  END;
END;
$$;




-- Étape 5 : Création de la tâche qui exécute la procédure tous les jours à 6h heure de Paris
CREATE OR REPLACE TASK DWH_RETAIL.BRONZE.TASK_DAILY_LOAD
  WAREHOUSE = COMPUTE_WH  -- Remplacer si besoin par ton warehouse
  SCHEDULE = 'USING CRON 0 4 * * * UTC'  -- 6h heure de Paris = 4h UTC
  COMMENT = 'Tâche quotidienne de chargement CRM/ERP avec notifications'
AS
  CALL DWH_RETAIL.BRONZE.SP_LOAD_ALL_DATA();




-- Étape 6 : Activation de la tâche planifiée

GRANT EXECUTE TASK ON ACCOUNT TO ROLE SYSADMIN;

ALTER TASK DWH_RETAIL.BRONZE.TASK_DAILY_LOAD RESUME;

DWH_RETAIL.BRONZE.ERP_CAT_G1V2