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
    prd_key varchar(50),
    prd_nm varchar(50),
    prd_cost int,
    prd_line varchar(50),
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

