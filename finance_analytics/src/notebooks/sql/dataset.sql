
DECLARE gb_divisor INT64 DEFAULT 1024*1024*1024;
DECLARE tb_divisor INT64 DEFAULT gb_divisor*1024;
DECLARE cost_per_tb INT64 DEFAULT 5;
DECLARE cost_factor FLOAT64 DEFAULT cost_per_tb / tb_divisor;
--> 11gb 0,02 formula de calculo para armazenamento de dados
DECLARE cost_storage_factor FLOAT64 DEFAULT 0.02;

--> previsao do custo acima de 1TB

SELECT
    project_id
    , dataset_id
    , table_id
    , row_count
    , size_bytes
    , ROUND(size_bytes/POW(10,6),2) AS size_mb
    , ROUND(size_bytes/POW(10,9),2) AS size_gb
    , IF(ROUND(size_bytes/POW(10,9),2)>10
            ,ROUND(ROUND(size_bytes/POW(10,9),2)*cost_storage_factor,6)
            ,0)  pre_cost_storage
    , ROUND(size_bytes/POW(10,12),2) AS size_tb    
    , IF(ROUND(size_bytes/POW(10,12),2)>1
            ,ROUND(ROUND(size_bytes/POW(10,12),2) * cost_per_tb, 6)
            ,0 ) AS pre_cost_us_dol          
    , TIMESTAMP_MILLIS(creation_time) AS creation_time
    , TIMESTAMP_MILLIS(last_modified_time) AS last_modified_time
    , CASE 
        WHEN type = 1 THEN 'table'
        WHEN type = 2 THEN 'view'
        WHEN type = 3 THEN 'external table'
        ELSE NULL
    END AS type
    , CURRENT_DATETIME("America/Sao_Paulo") AS bio_process_time
FROM `dev_domestico.__TABLES__`

where table_id in  ('credito_mes','custo_vida_2022','custo_consolidado_2022','recebimento_geral_2022','saldo_mes_2022')
