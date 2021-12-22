SELECT
    DATE,
    STORE_ID,
    ROUND((SUM(SP) - SUM(CP)),2) as ST_PROFIT FROM clean_store_transaction
GROUP BY DATE, STORE_ID ORDER BY ST_PROFIT DESC INTO OUTFILE  '/store_files_mysql/store_wise_daily_profit.csv'
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';

SELECT
    DATE,
    STORE_LOCATION,
    ROUND((SUM(SP) - SUM(CP)),2) as LC_PROFIT FROM clean_store_transaction
GROUP BY DATE, STORE_LOCATION ORDER BY LC_PROFIT DESC INTO OUTFILE  '/store_files_mysql/location_wise_daily_profit.csv'
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n';