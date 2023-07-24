/**

acompanhamento de tabelas 


**/

 create or replace table composed-night-232419.bee_analytics_transporte_monitoria.consolidado_info_tabelas
 as
select 
            u.access_date
            , u.user_email
            , u.tipo_user
            , t.project_id
            , t.dataset_id
            , t.table_id
            , t.data_ult_modificacao
            , replace(cast(round(t.size_tb,2) as string),'.',',') size_tb

             , ifnull(replace(cast(round(t.custo_query,2) as string),'.',','),'0') custo_query
             , ifnull(replace(cast(round(t.custo_storage,2) as string),'.',','),'0') custo_storage
             , ifnull(replace(cast(round(yearly_cost,2) as string),'.',','),'0') custo_ano_bee
             , data_carga

from (
select project_id
            , dataset_id
            , table_id
            , data_acesso
            , data_ult_modificacao
            , custo_query 
            , custo_storage 
            , size_tb
            , qtde_execucao
            , date(data_carga) data_carga
, row_number() over (win_analise) nrows
from composed-night-232419.bee_analytics_transporte_monitoria.historico_info_tabelas
where project_id = 'composed-night-232419'

      qualify nrows = 1

      window win_analise as (partition by  project_id
                              , dataset_id
                              , table_id
                              , date(data_carga) order by date(data_carga) desc)
) t
LEFT JOIN (

                select 
                        userx.project_id
                    , userx.dataset_id
                    , userx.table_id
                    , userx.access_date
                    , userx.user_email
                    , case when userx.extract_service_user_email = 'bio' then
                            'service account bio'
                            when userx.extract_service_user_email = 'service' then
                            'service segurança'      
                            when userx.extract_service_user_email is null and user_email in ('andre.barbosa@americanas.io','diogo.tsuruda@americanas.io','nicholas.pucci@americanas.io','suzane.carvalho@americanas.io','gerson.goulart@americanas.io') then
                            'usuario de dash'     
                            when userx.extract_service_user_email is null then
                            'usuario nominal'
                            else 'Na' end as tipo_user
                    , row_number() over(win_user) nrows                            
                from (
                        SELECT
                            'composed-night-232419' as project_id 
                            , T_USER.table_id
                            , T_USER.dataset_id
                            , T_USER.access_date
                            , T_USER.user_email
                            , REGEXP_EXTRACT(T_USER.user_email, r"^(service|bio)+[a-zA-Z0-9_.+-]+[^\\b]") AS extract_service_user_email
                            , CURRENT_DATETIME("America/Sao_Paulo") AS datahora_carga
                            , rank() over(win_user) nranks
                            
                        FROM `composed-night-232419.bee_analytics_transporte.table_users` T_USER

                        qualify nranks = 1

                        window win_user as (partition by T_USER.table_id, T_USER.dataset_id order by T_USER.access_date desc)
                ) userx
                
                qualify nrows = 1
                
                window win_user as (partition by userx.table_id, userx.dataset_id order by userx.access_date desc)
) u  ON (u.project_id = t.project_id
        and u.dataset_id = t.dataset_id
        and u.table_id = t.table_id)

left join b2w-bee-analytics.evaluated_bigquery_metadata.table_costs cst ON (cst.project_id = t.project_id
        and cst.dataset_id = t.dataset_id
        and cst.table_id = t.table_id)
