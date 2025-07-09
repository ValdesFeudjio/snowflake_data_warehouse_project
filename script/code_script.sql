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