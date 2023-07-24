/**

acompanhamento de tabelas - informação historico e diario

gravar dados do dia e fazer update nos dia atual

para montar mergedas colunas 

SELECT CONCAT('hist.',column_name,'=','temp.',column_name ) FROM bee_analytics_transporte_legado.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'information_acompanhamento_tabelas_atuais'

**/
begin

DECLARE gb_divisor INT64 DEFAULT 1024*1024*1024;
DECLARE tb_divisor INT64 DEFAULT gb_divisor*1024;
DECLARE cost_per_tb INT64 DEFAULT 5;
DECLARE cost_factor FLOAT64 DEFAULT cost_per_tb / tb_divisor;
DECLARE DATA_ATUAL DATETIME;


SET DATA_ATUAL = datetime_sub(CURRENT_DATEtime("America/Sao_Paulo"),  interval %(dias)s day)  ;

create temp table temp_monitoria_tabelas_ativas

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
        , REGEXP_EXTRACT(destination_table.table_id, r"^(anon)+[a-zA-Z0-9_.+-]+[^\\b]") AS executor_table_id_job
        , REGEXP_EXTRACT(parent_job_id, r"^(airflow)+[a-zA-Z0-9_.+-]+[^\\b]") AS executor_parent_airflow_job
        , REGEXP_EXTRACT(parent_job_id, r"^(scheduled_query)+[a-zA-Z0-9_.+-]+[^\\b]") AS executor_parent_cons_prog_job 
        , REGEXP_EXTRACT(job_id,r"(?i)(scheduled_query|script_job|airflow)+[a-zA-Z0-9_.+-]+[^\\b]") as executor
        , REGEXP_EXTRACT(destination_table.dataset_id,r"^(_script)+[a-zA-Z0-9_.+-]+[^\\b]") dataset
        , REGEXP_EXTRACT(destination_table.dataset_id,r"^(_[a-zA-Z0-9]{6})+[a-zA-Z0-9_.+-]+[^\\b]") dataset_others) info_extra
FROM region-us.INFORMATION_SCHEMA.JOBS
WHERE DATE(start_time, "America/Sao_Paulo") between '2022-01-01' and date(DATA_ATUAL)
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
        , e.executor_table_id_job
        , e.executor_parent_cons_prog_job
        , e.executor_parent_airflow_job
         , case when (e.executor_parent_cons_prog_job IS NOT NULL and e.executor_parent_cons_prog_job = 'scheduled_query') 
                or (e.executor_parent_cons_prog_job IS NULL and e.executor = 'scheduled_query') then
                    "consulta_programada"
               when (e.executor = 'script_job' and executor_parent_airflow_job = 'airflow') 
               or (e.executor = 'airflow') then 
                    "airflow"
               else "manual" end executor_job
        , case when e.dataset ="_script" then
                "temp"
                when e.dataset is null and e.dataset_others is not null then
                "temp"
                else e.dataset_id end dataset_id_job
from jobs j , unnest(info_extra) e

)



, custo_execucao_historico as(
select i.*  
from info i
inner join composed-night-232419.bee_analytics_transporte_monitoria.consolidado_info_tabelas d
on (i.project_id = d.project_id
    and i.dataset_id = d.dataset_id
     and i.table_id = d.table_id

     )


)


, custo_execucao_day as(
select i.*  
, d.data_carga
from info i
inner join composed-night-232419.bee_analytics_transporte_monitoria.consolidado_info_tabelas d
on (i.project_id = d.project_id
    and i.dataset_id = d.dataset_id
     and i.table_id = d.table_id
     and date(i.start_time) = date(d.data_carga)
 
     )

and DATE(start_time ) = date(DATA_ATUAL)
)


, historico as 
(
  select project_id
      , dataset_id
      , table_id

      , min(data_hora_execucao_job)  over(win_hist) pri_data_hora_execucao_job
      , max(data_hora_execucao_job)  over(win_hist) ulti_data_hora_execucao_job
      , sum(round(cost_us_dol,2))  over(win_hist) cost_us_dol
      , count(1) over(win_hist) qtde_execucao
      , row_number() over(win_hist) nrowsz
from (
      select 
            hist.project_id
            , hist.dataset_id
            , hist.table_id
            , hist.start_time data_hora_execucao_job
            , hist.cost_us_dol
            , sum(hist.cost_us_dol) over(win_hist_2) cust
            , row_number() over(win_hist) nrowsz

      from custo_execucao_historico hist

     where  hist.cost_us_dol > 0

      qualify nrowsz = 1

      window win_hist as (partition by hist.project_id, hist.dataset_id, hist.table_id, hist.start_time
      order by hist.start_time desc)
      , win_hist_2 as (partition by hist.project_id, hist.dataset_id, hist.table_id)

      )
qualify nrowsz = 1 

window win_hist as (partition by project_id, dataset_id, table_id)
)


, dia as 
(
  select project_id
      , dataset_id
      , table_id
      , executor_job
      , min(data_hora_execucao_job)  over(win_hist) pri_data_hora_execucao_job
      , max(data_hora_execucao_job)  over(win_hist) ulti_data_hora_execucao_job
      , sum(round(cost_us_dol,2))  over(win_hist) cost_us_dol
      , count(1) over(win_hist) qtde_execucao
      , row_number() over(win_hist) nrowsz
from (
      select 
            hist.project_id
            , hist.dataset_id
            , hist.table_id
            , hist.data_carga
            , hist.executor_job
            , hist.start_time data_hora_execucao_job
            , hist.cost_us_dol
            , sum(hist.cost_us_dol) over(win_hist_2) cust
            , row_number() over(win_hist) nrowsz

      from custo_execucao_day hist

     where  hist.cost_us_dol > 0

      qualify nrowsz = 1

      window win_hist as (partition by hist.project_id, hist.dataset_id, hist.table_id, hist.start_time, hist.data_carga
      order by hist.start_time desc)
      , win_hist_2 as (partition by hist.project_id, hist.dataset_id, hist.table_id, hist.data_carga order by  hist.data_carga)

      )
qualify nrowsz = 1 

window win_hist as (partition by project_id, dataset_id, table_id, data_carga order by data_carga)
)

select 
            t.project_id
            ,t.dataset_id
            ,t.table_id
            ,t.data_ult_modificacao
            ,t.access_date
            ,t.size_tb
            , t.custo_ano_bee
            
            , h.qtde_execucao
            , ifnull(replace(cast(round(h.cost_us_dol,2) as string),'.',','),'0') custo_hist
            , h.pri_data_hora_execucao_job
            , h.ulti_data_hora_execucao_job
            , ifnull(replace(cast(round(h.cost_us_dol/h.qtde_execucao,2) as string),'.',','),'0') custo_query_execucao_job

            , d.executor_job executor_job_dia
            , d.qtde_execucao qtde_execucao_dia
            , ifnull(replace(cast(d.cost_us_dol as string),'.',','),'0') custo_dia
            , d.pri_data_hora_execucao_job pri_data_hora_execucao_job_dia
            , d.ulti_data_hora_execucao_job ulti_data_hora_execucao_job_dia
            , ifnull(replace(cast(round(d.cost_us_dol/d.qtde_execucao,2) as string),'.',','),'0') custo_query_execucao_dia

            , date(data_carga) data_carga

from composed-night-232419.bee_analytics_transporte_monitoria.consolidado_info_tabelas t
left join historico h on (t.project_id = h.project_id 
                              and t.dataset_id = h.dataset_id 
                              and t.table_id = h.table_id)

left join dia d on (t.project_id = d.project_id 
                              and t.dataset_id = d.dataset_id 
                              and t.table_id = d.table_id
                              and date(t.data_carga) = date(d.pri_data_hora_execucao_job)
                              and date(t.data_carga) = date(d.ulti_data_hora_execucao_job))

;end;


begin

DECLARE DATA_ATUAL DATETIME;


SET DATA_ATUAL = datetime_sub(CURRENT_DATEtime("America/Sao_Paulo"),  interval %(dias)s day)  ;


MERGE INTO composed-night-232419.bee_analytics_transporte_monitoria.monitoria_tabelas_ativas AS hist
USING (

select * from temp_monitoria_tabelas_ativas
where date(data_carga) = date(DATA_ATUAL)

 ) temp
 ON (temp.project_id = hist.project_id 
    and temp.dataset_id = hist.dataset_id 
    and temp.table_id = hist.table_id
    and temp.data_carga = hist.data_carga)

WHEN MATCHED AND date(hist.data_carga) = DATE(DATA_ATUAL)   THEN 

UPDATE SET

hist.project_id=temp.project_id
,hist.dataset_id=temp.dataset_id
,hist.table_id=temp.table_id
,hist.data_ult_modificacao=temp.data_ult_modificacao
,hist.access_date=temp.access_date
,hist.size_tb=temp.size_tb
,hist.custo_ano_bee=temp.custo_ano_bee
--,hist.qtde_execucao=temp.qtde_execucao
--,hist.custo_hist=temp.custo_hist
--,hist.pri_data_hora_execucao_job=temp.pri_data_hora_execucao_job
--,hist.ulti_data_hora_execucao_job=temp.ulti_data_hora_execucao_job
--,hist.custo_query_execucao_job=temp.custo_query_execucao_job
,hist.executor_job_dia=temp.executor_job_dia
,hist.qtde_execucao_dia=temp.qtde_execucao_dia
,hist.custo_dia=temp.custo_dia
,hist.pri_data_hora_execucao_job_dia=temp.pri_data_hora_execucao_job_dia
,hist.ulti_data_hora_execucao_job_dia=temp.ulti_data_hora_execucao_job_dia
,hist.custo_query_execucao_dia=temp.custo_query_execucao_dia
,hist.data_carga=temp.data_carga


WHEN NOT MATCHED THEN
   INSERT

    (project_id,
dataset_id,
table_id,
data_ult_modificacao,
access_date,
size_tb,
custo_ano_bee,
qtde_execucao,
custo_hist,
pri_data_hora_execucao_job,
ulti_data_hora_execucao_job,
custo_query_execucao_job,
executor_job_dia,
qtde_execucao_dia,
custo_dia,
pri_data_hora_execucao_job_dia,
ulti_data_hora_execucao_job_dia,
custo_query_execucao_dia,
data_carga)

values(
temp.project_id,
temp.dataset_id,
temp.table_id,
temp.data_ult_modificacao,
temp.access_date,
temp.size_tb,
temp.custo_ano_bee,
temp.qtde_execucao,
temp.custo_hist,
temp.pri_data_hora_execucao_job,
temp.ulti_data_hora_execucao_job,
temp.custo_query_execucao_job,
temp.executor_job_dia,
temp.qtde_execucao_dia,
temp.custo_dia,
temp.pri_data_hora_execucao_job_dia,
temp.ulti_data_hora_execucao_job_dia,
temp.custo_query_execucao_dia,
temp.data_carga --TIMESTAMP(CURRENT_DATETIME("America/Sao_Paulo"))
);

END;
