create or replace table devsamelo2.dev_domestico.custo_vida_2022 as 

/*
CORREÇÕES:
  update  devsamelo2.dev_domestico.custo_2020 
set tipo_custo = 'seminario'
where dt_pagto_bq = '2020-10-01'
  and custo = 'INT TED  554013 SEMI'

  UPDATE  `devsamelo2.dev_domestico.custo_2021_excel` 
set tipo_custo = 'seminario' 
where regexp_extract(custo, r'^[aA-zZ0-9]+[\\ b]+[aA-zZ0-9]+[\\ b]+[/(I)/]+[/(M)/]+[/(W)/]+[\\ b]+[/(R)/]') = 'PIX TRANSF  IMW R'

--> PAGO PELA POUPANÇA NÃO ENTROU NO CONTROLE DE CUSTO

Tipo de conta: conta poupança
Agência: 9293	Conta: 22915-0
Valor: 570,00
Nome Favorecido: IMW R 3 CEFORTE
Documento Favorecido: 13.823.676/0073-00
Instituição Favorecido: Bco Bradesco S.a.
Agência Favorecido: 2741
Conta Favorecido: 00000047311-1
Tipo Conta Favorecido: conta corrente
*/

with custo_2021 as 
    (select --* except(ordem)  
     tipo_custo
        , custo
        ,	sum(valor_custo) valor_custo
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
    where ordem = 1
    group by tipo_custo
    , custo

        ,	dt_mes_base
        ,	dt_custo)





select 
dt_mes_base 

        ,custo
        ,tipo_custo
        ,dt_custo
        ,extract(week from dt_mes_base) semana_base        
        ,extract(year from dt_mes_base) ano_base
        ,extract(month from dt_mes_base) mes_base_ordem
        ,case when extract(month from dt_mes_base) = 1 then 'JAN' 
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
    , source
    ,	valor_custo

from (
    select 
        dt_mes_base ,

        custo,
        tipo_custo,
        dt_custo,
        valor_custo,
        'debito' as source 

from custo_2021

union all

(
        select dt_credito dt_mes_base,
                custo_credito custo,
                tipo_custo_credito tipo_custo,

                dt_credito ,
            sum(valor_credito_parc) valor_custo,
            'credito' as source 
    from (
    select tipo_custo_credito,
    custo_credito,
            dt_credito , 
            valor_credito_parc,
    row_number() over (partition by custo_credito,	dt_credito	, cast(valor_credito_parc as string),	dt_mes_base order by process_time desc) ordem
     from `devsamelo2.dev_domestico.credito_2022_excel`
   -- where dt_mes_base = '2021-10-01'
    ) where ordem = 1
    group by dt_credito ,
            custo_credito,
            tipo_custo_credito,

            dt_credito 
)



union all

(
        select dt_credito dt_mes_base,
                custo_credito custo,
                tipo_custo_credito tipo_custo,

                dt_credito ,
            sum(valor_credito_parc) valor_custo,
            'credito' as source 
    from (
    select tipo_custo_credito,
    custo_credito,
            dt_credito , 
            valor_credito_parc,
    row_number() over (partition by custo_credito,	dt_credito	, cast(valor_credito_parc as string),	dt_mes_base order by process_time desc) ordem
     from `devsamelo2.dev_domestico.credito_2023_excel`
   -- where dt_mes_base = '2021-10-01'
    ) where ordem = 1
    group by dt_credito ,
            custo_credito,
            tipo_custo_credito,

            dt_credito 
)
) 
where dt_mes_base >= '2022-01-01' --,'2021-09-01','2021-07-01','2021-06-01')
and tipo_custo in ('mercado','alimentação','compras','farmacia')
--group by dt_mes_base, tipo_custo, dt_custo, mes_base_ordem, mes_base, ano_base

order by dt_mes_base
