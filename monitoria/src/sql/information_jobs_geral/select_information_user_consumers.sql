with acess_user as (
SELECT
    'composed-night-232419' as project_id 
    , T_USER.table_id
    , T_USER.dataset_id

     , T_USER.access_date
    , T_USER.user_email

    , REGEXP_EXTRACT(T_USER.user_email, r"^(service|bio|engcore|b2w)+[a-zA-Z0-9_.+-]+[^\\b]") AS extract_service_user_email
     
    , CURRENT_DATETIME("America/Sao_Paulo") AS datahora_carga
    , rank() over(win_user) nranks
    , row_number() over(win_user) nrows
 FROM `composed-night-232419.bee_analytics_transporte.table_users` T_USER
 where T_USER.table_id  in (%(table_id)s)

 --and T_USER.access_date  = '2023-03-19'

 qualify nranks = 1

 window win_user as (partition by T_USER.table_id, T_USER.dataset_id order by T_USER.access_date desc)

)

, user_chave as (
select 
   project_id
    
    , dataset_id
    , table_id
    , max(access_date) over(win_user) access_date
    , array(
            select as struct 
                        project_id
                        
                        , dataset_id
                        , table_id
                        , access_date
                        , user_email
                        --> marcar o tipo de usuario que conecta na tabela
                        , case when extract_service_user_email = 'bio' then
                              'service account bio'
                              when extract_service_user_email = 'service' then
                              'service segurança'      
                              when extract_service_user_email is null and user_email in ('andre.barbosa@americanas.io','diogo.tsuruda@americanas.io','nicholas.pucci@americanas.io','suzane.carvalho@americanas.io') then
                              'usuario de dash'     
                              when extract_service_user_email is null then
                              'usuario nominal'
                              else 'Na' end as tipo_user
                        from acess_user r
                        where r.project_id = xr.project_id
                        and r.dataset_id = xr.dataset_id
                        and r.table_id = xr.table_id
                        ) user
      , row_number() over(win_user) as nrows
from acess_user xr

qualify     nrows = 1

window win_user as (partition by xr.table_id, xr.dataset_id, xr.access_date order by xr.access_date desc)
)




select u.* except(user, nrows)
, if(ux.user_email is not null, true, false) flag_user_sa_bio
, if(uz.user_email is not null, true, false) flag_user_nominal
, if(uxz.user_email is not null, true, false) flag_sem_acesso

, row_number() over(win_user) as nrows
from user_chave u
left join unnest(user) ux on ( u.project_id = ux.project_id
                        and u.dataset_id = ux.dataset_id
                        and u.table_id = ux.table_id
                        and u.access_date = ux.access_date
                        and ux.tipo_user = 'service account bio')
left join unnest(user) uz on ( u.project_id = uz.project_id
                        and u.dataset_id = uz.dataset_id
                        and u.table_id = uz.table_id
                        and u.access_date = uz.access_date
                        and uz.tipo_user = 'usuario nominal')
left join unnest(user) uxz on ( u.project_id = uxz.project_id
                        and u.dataset_id = uxz.dataset_id
                        and u.table_id = uxz.table_id
                        and u.access_date = uxz.access_date
                        and uxz.tipo_user = 'service segurança')

qualify     nrows = 1

window win_user as (partition by u.table_id, u.dataset_id, u.access_date order by u.access_date desc)

order by u.access_date desc
