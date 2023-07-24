
DECLARE gb_divisor INT64 DEFAULT 1024*1024*1024;
DECLARE tb_divisor INT64 DEFAULT gb_divisor*1024;
DECLARE cost_per_tb INT64 DEFAULT 5;
DECLARE cost_factor FLOAT64 DEFAULT cost_per_tb / tb_divisor;
DECLARE DATA_PERIODO_JOBS DATE;


set DATA_PERIODO_JOBS = DATE_SUB(CURRENT_DATE("America/Sao_Paulo"), INTERVAL 1 day);


--(query, r"[^\\b]CREATE[^\\b]TABLE[^\\b]IF[^\\b]NOT[^\\b]EXISTS[^\\b]+[a-zA-Z0-9_.+-]+[^\\b]")
--DECLARE pattern_sql STRING DEFAULT r"(?i)(create or replace table|create table if not exists|create temp table|merge into|merge)(?:\s+\x60?)(?:composed-night-232419.)?(?:.+?)(?:\x60?\s|\()";

 create or replace table dev_monitoria.information_executor_jobs_finance
 as

with jobs as (
SELECT  statement_type
        , user_email
        , job_id
        , job_type
        , DATETIME(start_time, "America/Sao_Paulo") start_time
        , query
        , state
        , total_bytes_billed
        , total_slot_ms
        , transferred_bytes
        , parent_job_id
        , TIMESTAMP_DIFF(end_time, start_time, SECOND) AS time_in_seconds
        , ROUND(( total_bytes_billed / tb_divisor ), 6) AS tb_billed
        , ROUND(( total_bytes_billed / tb_divisor ) * cost_per_tb, 6) AS cost_us_dol
        , ( total_slot_ms / 1000) total_slot 
        , ARRAY(SELECT AS STRUCT job_id
        , destination_table.project_id
        , destination_table.dataset_id
        , destination_table.table_id
        , REGEXP_EXTRACT(destination_table.table_id, r"^(anon)+[a-zA-Z0-9_.+-]+[^\\b]") AS executor_anon_job
        , REGEXP_EXTRACT(parent_job_id, r"^(airflow)+[a-zA-Z0-9_.+-]+[^\\b]") AS executor_parent_airflow_job
        , REGEXP_EXTRACT(parent_job_id, r"^(scheduled_query)+[a-zA-Z0-9_.+-]+[^\\b]") AS executor_parent_cons_prog_job 
        , REGEXP_EXTRACT(job_id,r"(?i)(scheduled_query|script_job|airflow)+[a-zA-Z0-9_.+-]+[^\\b]") as executor
        , REGEXP_EXTRACT(destination_table.dataset_id,r"^(_script)+[a-zA-Z0-9_.+-]+[^\\b]") dataset
        , REGEXP_EXTRACT(destination_table.dataset_id,r"^(_[a-zA-Z0-9]{6})+[a-zA-Z0-9_.+-]+[^\\b]") dataset_others) info_extra

FROM region-us.INFORMATION_SCHEMA.JOBS
WHERE DATE(start_time, "America/Sao_Paulo") >= DATA_PERIODO_JOBS
AND destination_table.table_id in  ('credito_mes','custo_vida_2022','custo_consolidado_2022','recebimento_geral_2022','saldo_mes_2022')
)

,info as (
SELECT  statement_type
        , user_email
        , j.job_id
        , job_type
        , start_time
        , query
        , state
        , total_bytes_billed
        , total_slot_ms
        , transferred_bytes
        , parent_job_id
        ,  time_in_seconds
        ,  tb_billed
        ,  cost_us_dol
        ,  total_slot 
        , e.project_id
        , e.dataset_id
        , e.table_id
        , e.dataset
        , e.dataset_others
        , e.executor_anon_job
        , e.executor_parent_cons_prog_job
        , e.executor_parent_airflow_job

         , case when (e.executor_parent_cons_prog_job IS NOT NULL and e.executor_parent_cons_prog_job = 'scheduled_query') 
                or (e.executor_parent_cons_prog_job IS NULL and e.executor = 'scheduled_query') then
                    "consulta_programada"
               when (e.executor = 'script_job' and executor_parent_airflow_job = 'airflow') or e.executor = 'airflow' then 
                    "airflow"
               when executor is not null then
                executor
               else "manual" end executor_job

        , case when e.dataset ="_script" then
                "temp"
                when e.dataset is null and e.dataset_others is not null then
                "temp"
                else e.dataset_id end dataset_id_job
from jobs j , unnest(info_extra) e

)

select *  
from info
WHERE (executor_anon_job not in ('anon') or executor_anon_job is null)
