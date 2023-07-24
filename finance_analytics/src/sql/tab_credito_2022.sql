/*alterado a forma de agrupamento por data para considerar data de compra e nao data base de pagamento
alterado tbm valor credito parcelado*/
create or replace table devsamelo2.dev_domestico.credito_mes as 
    select tipo_custo_credito
    , custo_credito
    , dt_mes_base
    , dt_credito
    ,     extract(year from dt_credito) ano_base
    , extract(month from dt_credito) mes_base_ordem
        , case when extract(month from dt_credito) = 1 then 'JAN' 
            when extract(month from dt_credito) = 2 then 'FEV' 
            when extract(month from dt_credito) = 3 then 'MAR' 
            when extract(month from dt_credito) = 4 then 'ABR' 
            when extract(month from dt_credito) = 5 then 'MAI' 
            when extract(month from dt_credito) = 6 then 'JUN' 
            when extract(month from dt_credito) = 7 then 'JUL' 
            when extract(month from dt_credito) = 8 then 'AGO' 
            when extract(month from dt_credito) = 9 then 'SET' 
            when extract(month from dt_credito) = 10 then 'OUT' 
            when extract(month from dt_credito) = 11 then 'NOV' 
            when extract(month from dt_credito) = 12 then 'DEZ' 
            else null end mes_base
    , sum(valor_credito_parc) valor_credito

    from (
    select tipo_custo_credito,custo_credito, dt_credito , dt_mes_base,valor_credito_parc,
    row_number() over (partition by custo_credito,	dt_credito	, cast(valor_credito_parc as string),	dt_mes_base order by process_time desc) ordem
     from `devsamelo2.dev_domestico.credito_2021_excel`
   -- where dt_mes_base = '2021-10-01'
   union all
    select tipo_custo_credito,custo_credito, dt_credito , dt_mes_base,valor_credito_parc,
    row_number() over (partition by custo_credito,	dt_credito	, cast(valor_credito_parc as string),	dt_mes_base order by process_time desc) ordem
     from `devsamelo2.dev_domestico.credito_2022_excel`
   -- where dt_mes_base = '2021-10-01'  
   union all
    select tipo_custo_credito,custo_credito, dt_credito , dt_mes_base,valor_credito_parc,
    row_number() over (partition by custo_credito,	dt_credito	, cast(valor_credito_parc as string),	dt_mes_base order by process_time desc) ordem
     from `devsamelo2.dev_domestico.credito_2023_excel`
   -- where dt_mes_base = '2021-10-01'    
    ) where ordem = 1

   group by tipo_custo_credito, custo_credito, dt_credito, dt_mes_base
    order by  dt_mes_base--,dt_credito
