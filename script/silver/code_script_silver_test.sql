/*
Feuille de test
*/
-- Cas de la table crm_cust_info

--check for nulls or duplicates in primary key

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

select cst_gndr
from bronze.crm_cust_info
Where cst_gndr != trim(cst_gndr); -- ici trim permet de supprimer les espaces en début et en fin des expressions


































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