/**CUSTO DE FEV: PIX TRANSF ESPACO 21/02
 TEM VALORES IGUAIS NO EXCEL - ALTERAR NOMES PARA CONTEMPLAR**/

create or replace table devsamelo2.dev_domestico.custo_consolidado_2022 as 
with custo_2021 as 
    (select tipo_custo
        ,	custo
        ,	valor_custo
        ,	dt_mes_base
        ,	dt_custo   
      from (
    select tipo_custo
        ,	custo
        ,	valor_custo
        ,	dt_mes_base
        ,	dt_custo
        , row_number() over (partition by custo,	cast(valor_custo as string),	dt_mes_base,	dt_custo order by process_time desc) ordem
    from `devsamelo2.dev_domestico.custo_2021_excel`) 
    where ordem = 1)

, custo_2022 as 
    (select tipo_custo
        ,	custo
        ,	valor_custo
        ,	dt_mes_base
        ,	dt_custo   
      from (
    select tipo_custo
        ,	custo
        ,	valor_custo
        ,	dt_mes_base
        ,	dt_custo
        , row_number() over (partition by custo,	cast(valor_custo as string),	dt_mes_base,	dt_custo order by process_time desc) ordem
    from `devsamelo2.dev_domestico.custo_2022_excel`) 
    where ordem = 1)    

, custo_2023 as 
    (select tipo_custo
        ,	custo
        ,	valor_custo
        ,	dt_mes_base
        ,	dt_custo   
      from (
    select tipo_custo
        ,	custo
        ,	valor_custo
        ,	dt_mes_base
        ,	dt_custo
        , row_number() over (partition by custo,	cast(valor_custo as string),	dt_mes_base,	dt_custo order by process_time desc) ordem
    from `devsamelo2.dev_domestico.custo_2023_excel`) 
    where ordem = 1) 

,hist as(
select *,
extract(year from dt_mes_base) ano_base,
extract(month from dt_mes_base) mes_base_ordem,
case when extract(month from dt_mes_base) = 1 then 'JAN' 
    when extract(month from dt_mes_base) = 2 then 'FEV' 
    when extract(month from dt_mes_base) = 3 then 'MAR' 
    when extract(month from dt_mes_base) = 4 then 'ABR' 
    when extract(month from dt_mes_base) = 5 then 'MAI' 
    when extract(month from dt_mes_base) = 6 then 'JUN' 
    when extract(month from dt_mes_base) = 7 then 'JUL' 
    when extract(month from dt_mes_base) = 8 then 'AGO' 
    when extract(month from dt_mes_base) = 9 then 'SET' 
    when extract(month from dt_mes_base) = 10 then 'OUT' 
    when extract(month from dt_mes_base) = 11 then 'NOV' 
    when extract(month from dt_mes_base) = 12 then 'DEZ' 
    else null end mes_base
from (


select 
dt_mes_base dt_mes_base,

custo,
tipo_custo,
dt_custo dt_custo_bq,
valor_custo,
from custo_2021

union all 

select 
dt_mes_base dt_mes_base,

custo,
tipo_custo,
dt_custo dt_custo_bq,
valor_custo,
from custo_2022

union all 

select 
dt_mes_base dt_mes_base,

custo,
tipo_custo,
dt_custo dt_custo_bq,
valor_custo,
from custo_2023

)

)

select * , 'consolidado' as source from hist
union all
SELECT data_base_bq dt_mes_base
       , custo
       , tipo_custo
       , dt_custo_bq
       , valor_custo
       , ano_base
       , mes_base_ordem
       , mes_base
       , 'previsão' as source 
FROM `devsamelo2.dev_domestico.custo_forms`
where pendente = 'Sim'
  and ano_base = 2023
