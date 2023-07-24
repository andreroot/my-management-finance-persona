

/**

TABELA GERADA PARA INICIAR O HISTORICO DOS DADOS DOS JOBS E 
ARMAZENAR UM HISTORICO DO DIA ANTERIORO DE CUSTO, 
USANDO A DATA DE MODIFICAÇÃO QUE EXISTE EM TODAS AS TABELAS

OBJETIVO MARCAR TABELAS E IDENTIFICAR 4 SITUAÇÕES:
-> TABELA ATUALIZADA por JOB E COM ACESSO ATUAL, GERANDO UM CUSTO
-> TABELA ATUALIZADA por JOB E SEM ACESSO, GERANDO UM CUSTO
-> TABELA DESATUALIZADA SEM JOB E COM ACESSO ATUAL
-> TABELA DESATUALIZADA SEM JOB E SEM ACESSO

NA SUBQUERIE APLICAR REGARA PARA GARANTIR QUE TABELA QUE FOI EXECUTADA NO DIA ANETRIOR E NO DIA CORRENTE NÃO
DEVE APARECER PARA CONTROLE DE EXECUÇÕES E TER UMA MONITORIA NA PREVISÃO DE EXECUÇÕES
**/

BEGIN

DECLARE DATA_ATUAL DATETIME;

SET DATA_ATUAL = datetime_sub(CURRENT_DATEtime("America/Sao_Paulo"),  interval %(dias)s day)  ;

MERGE composed-night-232419.bee_analytics_transporte_monitoria.historico_info_tabelas hist

USING (

with job as (

select 
      project_id
      ,dataset_id
      ,table_id
      , size_tb
      ,pre_cost_storage

      , executor_job
      , data_acesso
      , flag_user_sa_bio
      , flag_user_nominal
      , flag_sem_acesso
      , max( hist.data_hora_execucao_job ) over(win_hist) data_hora_execucao_job
-- --> data_ult_modificacao: info em todas as tabelas independente se existi job, existe uma data de modificação
       , data_ult_modificacao
      , row_number() over(win_hist) nrowsz

from composed-night-232419.bee_analytics_transporte.information_details_jobs hist

where date(hist.data_hora_execucao_job) = date( DATA_ATUAL  )
and project_id = 'composed-night-232419'


qualify nrowsz = 1

window win_hist as 
(partition by hist.project_id, hist.dataset_id, hist.table_id, hist.executor_job, date(hist.data_hora_execucao_job) order by  hist.data_hora_execucao_job desc)


)

, job_cust as (


select project_id
      , dataset_id
      , table_id
      , executor_job
      , max(data_hora_execucao_job)  over(win_hist) data_hora_execucao_job
      , max(data_hora_execucao_job)  over(win_hist) data_ult_modificacao
      , sum(round(cost_us_dol,2))  over(win_hist) cost_us_dol
      , count(1) over(win_hist) qtde_execucao
      , row_number() over(win_hist) nrowsz
from (
      select 
            hist.project_id, hist.dataset_id, hist.table_id, hist.executor_job
            , hist.data_hora_execucao_job
            , hist.cost_us_dol
            , sum(hist.cost_us_dol) over(win_hist_2) cust
            , row_number() over(win_hist) nrowsz

      from composed-night-232419.bee_analytics_transporte.information_details_jobs hist

     where date(hist.data_hora_execucao_job) =  date(DATA_ATUAL )
       and hist.cost_us_dol > 0
      and project_id = 'composed-night-232419'


      qualify nrowsz = 1

      window win_hist as (partition by hist.project_id, hist.dataset_id, hist.table_id, hist.executor_job, hist.data_hora_execucao_job
      order by hist.data_hora_execucao_job desc)
      , win_hist_2 as (partition by hist.project_id, hist.dataset_id, hist.table_id, hist.executor_job)

      )
qualify nrowsz = 1 

window win_hist as (partition by project_id, dataset_id, table_id, executor_job)
)

, job_acess_cust as (
select 
      j.project_id
      ,j.dataset_id
      ,j.table_id
      
      ,ifnull(j.data_acesso,'2001-01-01') data_acesso
      , j.flag_user_sa_bio
      , j.flag_user_nominal
      , j.flag_sem_acesso
      ,ifnull(tj.data_ult_modificacao,j.data_ult_modificacao) data_ult_modificacao 
      ,j.executor_job
      ,j.data_hora_execucao_job
      , j.size_tb

      , j.pre_cost_storage
      ,tj.cost_us_dol cust
      ,tj.qtde_execucao

      , 'novo' as status
      , ifnull(tj.data_hora_execucao_job, DATA_ATUAL ) as data_carga

from job j  
inner join job_cust tj
on (tj.project_id = j.project_id 
and tj.dataset_id = j.dataset_id 
and tj.table_id = j.table_id 
and tj.executor_job = j.executor_job
 and tj.data_hora_execucao_job = j.data_hora_execucao_job)
)

, job_not_acess_cust as (

            select 
                  project_id
                  ,dataset_id
                  ,table_id

                  ,ifnull(data_acesso,'2001-01-01') data_acesso
                  , flag_user_sa_bio
                  , flag_user_nominal   
                  , flag_sem_acesso    
                
                  , data_ult_modificacao


                  , 'null' executor_job  
                  , IFNULL(data_hora_execucao_job,datetime('2020-01-01 00:00:00.000000')) data_hora_execucao_job
                  , size_tb
                  
                  , pre_cost_us_dol custo_query
                  , pre_cost_storage custo_storage
                  , 0 qtde_execucao
                  , 'novo' status
                  
                  , DATA_ATUAL as data_carga
                  , row_number() over(win_hist) nrowsz
            from composed-night-232419.bee_analytics_transporte.information_details_jobs hist

            where hist.table_id not in (
                  select distinct table_id
                  from bee_analytics_transporte.information_details_jobs where date(data_hora_execucao_job) = date(DATA_ATUAL)
                  )
            and (hist.data_hora_execucao_job is null or date(data_hora_execucao_job) = date_sub(date(DATA_ATUAL), interval 1 day))
            and hist.data_ult_modificacao is not null
            and hist.project_id = 'composed-night-232419'

            qualify nrowsz = 1

            window win_hist as 
            (partition by hist.project_id, hist.dataset_id, hist.table_id, hist.executor_job, date(hist.data_hora_execucao_job) order by  hist.data_hora_execucao_job desc)



)

, all_tab as (
SELECT 
      project_id 
      , dataset_id 
      , table_id
      , data_acesso
      , flag_user_sa_bio
      , flag_user_nominal   
      , flag_sem_acesso    
      , data_ult_modificacao
      , executor_job 
      , data_hora_execucao_job

      , size_tb
      , cust                  custo_query
      , pre_cost_storage      custo_storage       
      , qtde_execucao
      , status
      , data_carga
FROM job_acess_cust
union all
SELECT          project_id 
      , dataset_id 
      , table_id
      , data_acesso
      , flag_user_sa_bio
      , flag_user_nominal   
      , flag_sem_acesso    
      , data_ult_modificacao
      , executor_job 
      , data_hora_execucao_job

      , size_tb
      , custo_query
      , custo_storage       
      , qtde_execucao
      , status
      , data_carga
FROM job_not_acess_cust
)

select * from all_tab

)  AS temp
ON (temp.project_id = hist.project_id
    and temp.dataset_id = hist.dataset_id
    and temp.table_id  = hist.table_id
    and temp.executor_job = hist.executor_job
    and temp.data_acesso = hist.data_acesso
    and temp.data_ult_modificacao  = hist.data_ult_modificacao
    AND date(temp.data_carga) = date(hist.data_carga))

WHEN MATCHED AND hist.status = "novo"  AND date(hist.data_carga) = DATE(DATA_ATUAL) THEN
    UPDATE SET

hist.project_id = temp.project_id
, hist.dataset_id = temp.dataset_id
, hist.table_id = temp.table_id
, hist.data_acesso = temp.data_acesso
, hist.flag_user_sa_bio = temp.flag_user_sa_bio
, hist.flag_user_nominal = temp.flag_user_nominal
, hist.flag_sem_acesso = temp.flag_sem_acesso
, hist.data_ult_modificacao = temp.data_ult_modificacao
, hist.executor_job = temp.executor_job
, hist.data_hora_execucao_job = temp.data_hora_execucao_job

, hist.size_tb = temp.size_tb
, hist.custo_query = temp.custo_query
, hist.custo_storage = temp.custo_storage
, hist.qtde_execucao = temp.qtde_execucao
, hist.status = 'atualizado'
, hist.data_carga = temp.data_carga 

WHEN NOT MATCHED  THEN 
    INSERT

    (project_id 
, dataset_id 
, table_id
, data_acesso
, flag_user_sa_bio
, flag_user_nominal
, flag_sem_acesso
, data_ult_modificacao
, executor_job 
, data_hora_execucao_job

, size_tb
, custo_query
, custo_storage
, qtde_execucao
, status
, data_carga )
values(
  temp.project_id
, temp.dataset_id 
, temp.table_id 
, temp.data_acesso 
, temp.flag_user_sa_bio
, temp.flag_user_nominal
, temp.flag_sem_acesso
, temp.data_ult_modificacao
, temp.executor_job
, temp.data_hora_execucao_job
, temp.size_tb
, temp.custo_query
, temp.custo_storage
, temp.qtde_execucao
, temp.status
, temp.data_carga --TIMESTAMP(CURRENT_DATETIME("America/Sao_Paulo"))
);

END;