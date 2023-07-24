create or replace table devsamelo2.dev_domestico.recebimento_geral_2022 as

with recebido_2022 as 
    (
        select descricao
        , dt_recebido 
        , dt_mes_base
        , valor_recebido  
        , ordem_recebimento  
      from (
    select descricao
        , dt_recebido 
        , dt_mes_base
        , valor_recebido
        , row_number() over (partition by descricao,	dt_recebido	, cast(valor_recebido as string),	dt_mes_base order by process_time desc) ordem
        , row_number() over (partition by 	dt_mes_base	 order by dt_recebido desc) ordem_recebimento
    from `devsamelo2.dev_domestico.recebido_2023_excel`
    union all        
    select descricao
        , dt_recebido 
        , dt_mes_base
        , valor_recebido
        , row_number() over (partition by descricao,	dt_recebido	, cast(valor_recebido as string),	dt_mes_base order by process_time desc) ordem
        , row_number() over (partition by 	dt_mes_base	 order by dt_recebido desc) ordem_recebimento
    from `devsamelo2.dev_domestico.recebido_2022_excel`
    union all
        select descricao
        , dt_recebido 
        , dt_mes_base
        , valor_recebido
        , row_number() over (partition by descricao,	dt_recebido	, cast(valor_recebido as string),	dt_mes_base order by process_time desc) ordem
        , row_number() over (partition by 	dt_mes_base	 order by dt_recebido desc) ordem_recebimento
    from `devsamelo2.dev_domestico.recebido_2021_excel`) 
    where ordem = 1
    )

, recebido_prev as (
    select descricao
        , dt_recebido 
        , dt_mes_base
        , valor_recebido
        , 1
from     `devsamelo2.dev_domestico.recebido_forms`
where dt_recebido >= current_date("America/Sao_Paulo")
)    

    select hist.* except(previsao)
        , prev.descricao as fonte_previsao
        , prev.dt_recebido as dt_recebido_previsao
        , prev.valor_recebido as valor_recebido_previsao
        , if(prev.valor_recebido is null, 'Historico', 'Previsto') as source
        , current_timestamp() data_atualizacao_jobs

    from (
        SELECT hist.*
             , array_agg(struct(prev.descricao, prev.dt_recebido , prev.valor_recebido)) previsao ---  as dt_recebido_previsao as valor_recebido_previsao

        FROM recebido_2022 hist

        left join recebido_prev prev 
        on  (hist.dt_mes_base = prev.dt_mes_base 
        and hist.dt_recebido = prev.dt_recebido 
        and hist.valor_recebido = prev.valor_recebido)

        where hist.dt_mes_base >= '2021-01-01'

        group by descricao
            , dt_recebido 
            , dt_mes_base
            , valor_recebido
            , ordem_recebimento
        --SELECT * FROM `devsamelo2.dev_domestico.recebido_forms_2021`
    ) hist, unnest(previsao) prev






