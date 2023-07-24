/*alteracao 12/05 - inclusao de coluna com a previsao do recebimento baseado no forms de recebimento */
/*incluido 2023 - 23/01*/
BEGIN

DECLARE JAN_2021 DATE DEFAULT '2021-01-01';
DECLARE FEV_2021 DATE DEFAULT '2021-02-01';
DECLARE MAR_2021 DATE DEFAULT '2021-03-01';
DECLARE ABR_2021 DATE DEFAULT '2021-04-01';
DECLARE MAI_2021 DATE DEFAULT '2021-05-01';
DECLARE JUN_2021 DATE DEFAULT '2021-06-01';
DECLARE JUL_2021 DATE DEFAULT '2021-07-01';
DECLARE AGO_2021 DATE DEFAULT '2021-08-01';
DECLARE SET_2021 DATE DEFAULT '2021-09-01';
DECLARE OUT_2021 DATE DEFAULT '2021-10-01';
DECLARE NOV_2021 DATE DEFAULT '2021-11-01';
DECLARE DEZ_2021 DATE DEFAULT '2021-12-01';

DECLARE JAN_2022 DATE DEFAULT '2022-01-01';
DECLARE FEV_2022 DATE DEFAULT '2022-02-01';
DECLARE MAR_2022 DATE DEFAULT '2022-03-01';
DECLARE ABR_2022 DATE DEFAULT '2022-04-01';
DECLARE MAI_2022 DATE DEFAULT '2022-05-01';
DECLARE JUN_2022 DATE DEFAULT '2022-06-01';
DECLARE JUL_2022 DATE DEFAULT '2022-07-01';
DECLARE AGO_2022 DATE DEFAULT '2022-08-01';
DECLARE SET_2022 DATE DEFAULT '2022-09-01';
DECLARE OUT_2022 DATE DEFAULT '2022-10-01';
DECLARE NOV_2022 DATE DEFAULT '2022-11-01';
DECLARE DEZ_2022 DATE DEFAULT '2022-12-01';

DECLARE JAN_2023 DATE DEFAULT '2023-01-01';
DECLARE FEV_2023 DATE DEFAULT '2023-02-01';
DECLARE MAR_2023 DATE DEFAULT '2023-03-01';
DECLARE ABR_2023 DATE DEFAULT '2023-04-01';
DECLARE MAI_2023 DATE DEFAULT '2023-05-01';
DECLARE JUN_2023 DATE DEFAULT '2023-06-01';
DECLARE JUL_2023 DATE DEFAULT '2023-07-01';
DECLARE AGO_2023 DATE DEFAULT '2023-08-01';
DECLARE SET_2023 DATE DEFAULT '2023-09-01';


/*RECEBIMENTO*/
create  temp table receb as

    select dt_mes_base, 
            sum(valor_recebido) valor_recebido,
            sum(valor_recebido_prev) valor_recebido_prev

    from (
    select descricao, 
            dt_recebido , 
            dt_mes_base,
            valor_recebido,
            0 valor_recebido_prev,
            row_number() over (window_rec_2021) ordem
     from `devsamelo2.dev_domestico.recebido_2021_excel`
     qualify ordem = 1
     window window_rec_2021 as (partition by descricao,	dt_recebido	, cast(valor_recebido as string),	dt_mes_base order by process_time desc) 
   union all 

    select descricao, 
            dt_recebido , 
            dt_mes_base,
            valor_recebido,
            0 valor_recebido_prev,
            row_number() over (window_rec_2022) ordem
     from `devsamelo2.dev_domestico.recebido_2022_excel`
          qualify ordem = 1
     window window_rec_2022 as (partition by descricao,	dt_recebido	, cast(valor_recebido as string),	dt_mes_base order by process_time desc) 

   union all 

    select descricao, 
            dt_recebido , 
            dt_mes_base,
            valor_recebido,
            0 valor_recebido_prev,
            row_number() over (window_rec_2022) ordem
     from `devsamelo2.dev_domestico.recebido_2023_excel`
          qualify ordem = 1
     window window_rec_2022 as (partition by descricao,	dt_recebido	, cast(valor_recebido as string),	dt_mes_base order by process_time desc) 

   union all 
    select descricao ,
         dt_recebido ,
         dt_mes_base,
         0 valor_recebido,
         valor_recebido valor_recebido_prev,
         1 ordem
from     `devsamelo2.dev_domestico.recebido_forms`
where dt_recebido >= current_date('America/Sao_Paulo')

) group by dt_mes_base;


/*CUSTO*/

create  temp table  custo_base as

select dt_mes_base
,sum(custo_prev) custo_prev
,sum(custo_real) custo_real
from(
    select dt_mes_base, 
         IF(source='prev',valor_custo,0) custo_prev,
         IF(source='real',valor_custo,0)  custo_real          
      from (
        select tipo_custo,
                custo,	
                valor_custo,	
                dt_mes_base,	
                dt_custo,
                'real' as source,
                row_number() over (window_cst_2021) ordem
            from `devsamelo2.dev_domestico.custo_2021_excel`
     qualify ordem = 1
     window window_cst_2021 as (partition by custo,	cast(valor_custo as string),	dt_mes_base,	dt_custo order by process_time desc) 
union all
        select tipo_custo,
                custo,	
                valor_custo,	
                dt_mes_base,	
                dt_custo,
                'real' as source,
                row_number() over (window_cst_2022) ordem
            from `devsamelo2.dev_domestico.custo_2022_excel`  
      qualify ordem = 1
      window window_cst_2022 as (partition by custo,	cast(valor_custo as string),	dt_mes_base,	dt_custo order by process_time desc)       

union all
        select tipo_custo,
                custo,	
                valor_custo,	
                dt_mes_base,	
                dt_custo,
                'real' as source,
                row_number() over (window_cst_2023) ordem
            from `devsamelo2.dev_domestico.custo_2023_excel`  
      qualify ordem = 1
      window window_cst_2023 as (partition by custo,	cast(valor_custo as string),	dt_mes_base,	dt_custo order by process_time desc)     
   union all 
SELECT tipo_custo
       , custo

       , valor_custo
       , data_base_bq dt_mes_base
       , dt_custo_bq dt_custo
       ,'prev' as source
      , 1 ordem       
FROM `devsamelo2.dev_domestico.custo_forms`
where pendente = 'Sim'
  and ano_base = 2023                        
    ) 
)
    group by dt_mes_base;



/*SALDO DEVE CONTER SOMENTE O SALDO INICIAL DO ANO PARA SER CALCULADO COM SALDO DA MOVIMENTAÇÃO DA EXTRAÇAÕ DE DADOS*/

create  temp table  saldo as
select dt_mes_base, 
        sum(saldo) saldo
    from (
    SELECT *     FROM (
        select descricao,
               dt_recebido, 
               dt_mes_base, 
               saldo,
               process_time,
               row_number() over (window_sald_2021) ordem,
               row_number() over (partition by 	dt_mes_base order by dt_recebido, process_time asc) ordem_prim_sald
         from `devsamelo2.dev_domestico.saldo_2021_excel`
         WHERE saldo  is not null
         qualify ordem = 1 and ordem_prim_sald = 1
         window window_sald_2021 as (partition by  descricao,	dt_recebido	, cast(saldo as string),	dt_mes_base order by process_time desc)
      )

union all 
    SELECT *     FROM (
        select descricao,
               dt_recebido, 
               dt_mes_base, 
               saldo,
               process_time,
               row_number() over (window_sald_2022) ordem,
               row_number() over (partition by 	dt_mes_base order by dt_recebido, process_time asc) ordem_prim_sald
         from `devsamelo2.dev_domestico.saldo_2022_excel`
         WHERE saldo  is not null
         qualify ordem = 1 and ordem_prim_sald = 1
         window window_sald_2022 as (partition by  descricao,	dt_recebido	, cast(saldo as string),	dt_mes_base order by process_time desc)
      )
UNION all
/** INCLUIDO DATA DE CORTE MES DE JAN**/
    SELECT *     FROM (
        select descricao,
               dt_recebido, 
               dt_mes_base, 
               saldo,
               process_time,
               row_number() over (window_sald_2022) ordem,
               row_number() over (partition by 	dt_mes_base order by dt_recebido, process_time asc) ordem_prim_sald
         from `devsamelo2.dev_domestico.saldo_2023_excel`
         WHERE saldo  is not null
         qualify ordem = 1 and ordem_prim_sald = 1 and dt_mes_base = '2023-01-01'
         window window_sald_2022 as (partition by  descricao,	dt_recebido	, cast(saldo as string),	dt_mes_base order by process_time desc)
      )      
    )

    group by dt_mes_base
;

/*CRIAR TABELA COM AS ASSOCIAÇÕES*/

create or replace table devsamelo2.dev_domestico.saldo_mes_2022 as 

with  jan_2021 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        0 as valor_recebido_prev,
        0 as custo_prev,
        rec.custo_real, 
        sald.saldo saldo_inicial, 
        0 as saldo_prev,
        rec.valor_recebido-rec.custo_real + sald.saldo saldo_real,  
        FEV_2021 dt_mes_base_saldo 
    from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.custo_prev,
           cst.custo_real
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = JAN_2021 )
     where  rec.dt_mes_base = JAN_2021
    ) rec
left join saldo sald  
  on (rec.dt_mes_base = sald.dt_mes_base 
     and sald.dt_mes_base = JAN_2021 )
where  rec.dt_mes_base = JAN_2021
order by rec.dt_mes_base
)

, fev_2021 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        0 as valor_recebido_prev,
        0 as custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        0 as saldo_prev,
        rec.valor_recebido-rec.custo_real + sald.saldo_real saldo_real,  
        MAR_2021 dt_mes_base_saldo 
 from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.custo_prev,
           cst.custo_real         
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = FEV_2021 )
     where  rec.dt_mes_base = FEV_2021
    ) rec
left join jan_2021 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo 
     and rec.dt_mes_base = FEV_2021 )
where  rec.dt_mes_base = FEV_2021
order by rec.dt_mes_base
)

, mar_2021 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        0 as valor_recebido_prev,
        0 as custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        0 as saldo_prev,
        rec.valor_recebido-rec.custo_real + sald.saldo_real saldo_real,   
        ABR_2021 dt_mes_base_saldo 
 from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.custo_prev,
           cst.custo_real        
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = MAR_2021 )
     where  rec.dt_mes_base = MAR_2021
    ) rec
left join fev_2021 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo 
     and rec.dt_mes_base = MAR_2021 )
where  rec.dt_mes_base = MAR_2021
order by rec.dt_mes_base
)


, abr_2021 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        0 as valor_recebido_prev,
        0 as custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        0 as saldo_prev,
        rec.valor_recebido-rec.custo_real + sald.saldo_real saldo_real,   
        MAI_2021 dt_mes_base_saldo 
 from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.custo_prev,
           cst.custo_real 
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = ABR_2021 )
     where  rec.dt_mes_base = ABR_2021
    ) rec
left join mar_2021 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo 
     and rec.dt_mes_base = ABR_2021 )
where  rec.dt_mes_base = ABR_2021
order by rec.dt_mes_base
)

, mai_2021 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        0 as valor_recebido_prev,
        0 as custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        0 as saldo_prev,
        rec.valor_recebido-rec.custo_real + sald.saldo_real saldo_real,  
        JUN_2021 dt_mes_base_saldo 
 from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.custo_prev,
           cst.custo_real 
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = MAI_2021 )
     where  rec.dt_mes_base = MAI_2021
    ) rec
left join abr_2021 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo  
     and rec.dt_mes_base = MAI_2021 )
where  rec.dt_mes_base = MAI_2021
order by rec.dt_mes_base
)

, jun_2021 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        0 as valor_recebido_prev,
        0 as custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        0 as saldo_prev,
        rec.valor_recebido-rec.custo_real + sald.saldo_real saldo_real,  
        JUL_2021 dt_mes_base_saldo 
 from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.custo_prev,
           cst.custo_real 
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = JUN_2021 )
     where  rec.dt_mes_base = JUN_2021
    ) rec
left join mai_2021 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo  
     and rec.dt_mes_base = JUN_2021 )
where  rec.dt_mes_base = JUN_2021
order by rec.dt_mes_base
)


, jul_2021 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        0 as valor_recebido_prev,
        0 as custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        0 as saldo_prev,
        rec.valor_recebido-rec.custo_real + sald.saldo_real saldo_real,  
        AGO_2021 dt_mes_base_saldo 
 from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.custo_prev,
           cst.custo_real 
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = JUL_2021 )
     where  rec.dt_mes_base = JUL_2021
    ) rec
left join jun_2021 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo  
     and rec.dt_mes_base = JUL_2021 )
where  rec.dt_mes_base = JUL_2021
order by rec.dt_mes_base
)

, ago_2021 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        0 as valor_recebido_prev,
        0 as custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        0 as saldo_prev,
        rec.valor_recebido-rec.custo_real + sald.saldo_real saldo_real,  
        SET_2021 dt_mes_base_saldo 
 from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.custo_prev,
           cst.custo_real
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = AGO_2021 )
     where  rec.dt_mes_base = AGO_2021
    ) rec
left join jul_2021 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo  
     and rec.dt_mes_base = AGO_2021 )
where  rec.dt_mes_base = AGO_2021
order by rec.dt_mes_base
)

, set_2021 as (
select rec.dt_mes_base,
        rec.valor_recebido,
        0 as valor_recebido_prev, 
        0 as custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        0 as saldo_prev,
        rec.valor_recebido-rec.custo_real + sald.saldo_real saldo_real,  
        OUT_2021 dt_mes_base_saldo 
 from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.custo_prev,
           cst.custo_real 
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = SET_2021 )
     where  rec.dt_mes_base = SET_2021
    ) rec
left join ago_2021 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo  
     and rec.dt_mes_base = SET_2021 )
where  rec.dt_mes_base = SET_2021
order by rec.dt_mes_base
)

, out_2021 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        0 as valor_recebido_prev,
        0 as custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        0 as saldo_prev,
        rec.valor_recebido-rec.custo_real + sald.saldo_real saldo_real,  
        NOV_2021 dt_mes_base_saldo 
 from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.custo_prev,
           cst.custo_real 
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = OUT_2021 )
     where  rec.dt_mes_base = OUT_2021
    ) rec
left join set_2021 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo  
     and rec.dt_mes_base = OUT_2021 )
where  rec.dt_mes_base = OUT_2021
order by rec.dt_mes_base
)


, nov_2021 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        0 as valor_recebido_prev,
        0 as custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        0 as saldo_prev,
        rec.valor_recebido-rec.custo_real + sald.saldo_real saldo_real,  
        DEZ_2021 dt_mes_base_saldo 
 from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.custo_prev,
           cst.custo_real 
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = NOV_2021 )
     where  rec.dt_mes_base = NOV_2021
    ) rec
left join out_2021 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo  
     and rec.dt_mes_base = NOV_2021 )
where  rec.dt_mes_base = NOV_2021
order by rec.dt_mes_base
)

, dez_2021 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        0 as valor_recebido_prev,
        0 as custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        0 as saldo_prev,
        rec.valor_recebido-rec.custo_real + sald.saldo_real saldo_real,  
        JAN_2022 dt_mes_base_saldo 
 from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.custo_prev,
           cst.custo_real 
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = DEZ_2021 )
     where  rec.dt_mes_base = DEZ_2021
    ) rec
left join nov_2021 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo  
     and rec.dt_mes_base = DEZ_2021 )
where  rec.dt_mes_base = DEZ_2021
order by rec.dt_mes_base
)
--> VERIFICAR SALDO DE 2022 NAO FOI VALIDADO COM DEZ/2021
--> SEMPE INICIAR COM SALDO QUE VEM DO EXTRATO DO BANCO

, jan_2022 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        0 as valor_recebido_prev,
        0 as custo_prev,
        rec.custo_real, 
        sald.saldo saldo_inicial, 
        0 as saldo_prev,
        rec.valor_recebido-rec.custo_real + if(sald.saldo is null, 0, sald.saldo) saldo_real, 
        FEV_2022 dt_mes_base_saldo 
    from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.custo_prev,
           cst.custo_real 
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = JAN_2022 )
     where  rec.dt_mes_base = JAN_2022
    ) rec
left join saldo sald  
  on (rec.dt_mes_base = sald.dt_mes_base 
     and sald.dt_mes_base = JAN_2022 )
where  rec.dt_mes_base = JAN_2022
order by rec.dt_mes_base
)

, fev_2022 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        0 as valor_recebido_prev,
        0 as custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        0 saldo_prev, 
        rec.valor_recebido-rec.custo_real + if(sald.saldo_real is null, 0, sald.saldo_real) saldo_real,
        MAR_2022 dt_mes_base_saldo 
    from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.custo_prev,
           cst.custo_real 
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = FEV_2022 )
     where  rec.dt_mes_base = FEV_2022
    ) rec
left join jan_2022 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo 
     and rec.dt_mes_base = FEV_2022 )
where  rec.dt_mes_base = FEV_2022
order by rec.dt_mes_base
)

, mar_2022 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_recebido_prev,
        rec.custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        0 saldo_prev, 
        rec.valor_recebido-rec.custo_real + if(sald.saldo_real is null, 0, sald.saldo_real) saldo_real,
        ABR_2022 dt_mes_base_saldo 
    from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           rec.valor_recebido_prev,
           cst.custo_prev,
           cst.custo_real        
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = MAR_2022 )
     where  rec.dt_mes_base = MAR_2022
    ) rec
left join fev_2022 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo 
     and rec.dt_mes_base = MAR_2022 )
where  rec.dt_mes_base = MAR_2022
order by rec.dt_mes_base
)

, abr_2022 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_recebido_prev,
        rec.custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        0 saldo_prev, 
        rec.valor_recebido-rec.custo_real + if(sald.saldo_real is null, 0, sald.saldo_real) saldo_real,
        MAI_2022 dt_mes_base_saldo 
    from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           rec.valor_recebido_prev,
           cst.custo_prev,
           cst.custo_real        
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = ABR_2022 )
     where  rec.dt_mes_base = ABR_2022
    ) rec
left join mar_2022 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo 
     and rec.dt_mes_base = ABR_2022 )
where  rec.dt_mes_base = ABR_2022
order by rec.dt_mes_base
)

/*MES CORRENTE MANTER PREVISAO DOS RECEBIMENTOS, CUSTO E GERAR UMA PREVISÃO DO SALDO */


, mai_2022 as (

select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_recebido_prev,
        rec.custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        0 saldo_prev, 
        rec.valor_recebido-rec.custo_real + if(sald.saldo_real is null, 0, sald.saldo_real) saldo_real,
        JUN_2022 dt_mes_base_saldo 
    from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           rec.valor_recebido_prev,
           cst.custo_prev,
           cst.custo_real        
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = MAI_2022 )
     where  rec.dt_mes_base = MAI_2022
    ) rec
left join abr_2022 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo 
     and rec.dt_mes_base = MAI_2022 )
where  rec.dt_mes_base = MAI_2022
order by rec.dt_mes_base

)


, jun_2022 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_recebido_prev,
        rec.custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        0 saldo_prev, 
        rec.valor_recebido-rec.custo_real + if(sald.saldo_real is null, 0, sald.saldo_real) saldo_real,
        JUL_2022 dt_mes_base_saldo 
    from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           rec.valor_recebido_prev,
           cst.custo_prev,
           cst.custo_real        
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = JUN_2022 )
     where  rec.dt_mes_base = JUN_2022
    ) rec
left join mai_2022 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo 
     and rec.dt_mes_base = JUN_2022 )
where  rec.dt_mes_base = JUN_2022
order by rec.dt_mes_base
)


, jul_2022 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_recebido_prev,
        rec.custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        0 saldo_prev, 
        rec.valor_recebido-rec.custo_real + if(sald.saldo_real is null, 0, sald.saldo_real) saldo_real,
        AGO_2022 dt_mes_base_saldo 
    from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           rec.valor_recebido_prev,
           cst.custo_prev,
           cst.custo_real        
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = JUL_2022 )
     where  rec.dt_mes_base = JUL_2022
    ) rec
left join jun_2022 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo 
     and rec.dt_mes_base = JUL_2022 )
where  rec.dt_mes_base = JUL_2022
order by rec.dt_mes_base
)


, ago_2022 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_recebido_prev,
        rec.custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        0 saldo_prev, 
        rec.valor_recebido-rec.custo_real + if(sald.saldo_real is null, 0, sald.saldo_real) saldo_real,
        SET_2022 dt_mes_base_saldo 
    from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           rec.valor_recebido_prev,
           cst.custo_prev,
           cst.custo_real        
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = AGO_2022 )
     where  rec.dt_mes_base = AGO_2022
    ) rec
left join jul_2022 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo 
     and rec.dt_mes_base = AGO_2022 )
where  rec.dt_mes_base = AGO_2022
order by rec.dt_mes_base
)


, set_2022 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_recebido_prev,
        rec.custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        0 saldo_prev,  
        rec.valor_recebido-rec.custo_real + if(sald.saldo_real is null, 0, sald.saldo_real) saldo_real,
        OUT_2022 dt_mes_base_saldo 
    from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           rec.valor_recebido_prev,
           cst.custo_prev,
           cst.custo_real        
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = SET_2022 )
     where  rec.dt_mes_base = SET_2022
    ) rec
left join ago_2022 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo 
     and rec.dt_mes_base = SET_2022 )
where  rec.dt_mes_base = SET_2022
order by rec.dt_mes_base
)

, out_2022 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_recebido_prev,
        rec.custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        0 saldo_prev, 
        rec.valor_recebido-rec.custo_real + if(sald.saldo_real is null, 0, sald.saldo_real) saldo_real,
        NOV_2022 dt_mes_base_saldo 
    from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           rec.valor_recebido_prev,
           cst.custo_prev,
           cst.custo_real        
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = OUT_2022 )
     where  rec.dt_mes_base = OUT_2022
    ) rec
left join set_2022 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo 
     and rec.dt_mes_base = OUT_2022 )
where  rec.dt_mes_base = OUT_2022
order by rec.dt_mes_base
)



, nov_2022 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_recebido_prev,
        rec.custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        0 saldo_prev, 
        rec.valor_recebido-rec.custo_real + if(sald.saldo_real is null, 0, sald.saldo_real) saldo_real,
        DEZ_2022 dt_mes_base_saldo 
    from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           rec.valor_recebido_prev,
           cst.custo_prev,
           cst.custo_real        
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = NOV_2022 )
     where  rec.dt_mes_base = NOV_2022
    ) rec
left join out_2022 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo 
     and rec.dt_mes_base = NOV_2022 )
where  rec.dt_mes_base = NOV_2022
order by rec.dt_mes_base
)


, dez_2022 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_recebido_prev,
        rec.custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        0 saldo_prev, 
        rec.valor_recebido-rec.custo_real + if(sald.saldo_real is null, 0, sald.saldo_real) saldo_real,
        JAN_2023 dt_mes_base_saldo 
    from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           rec.valor_recebido_prev,
           cst.custo_prev,
           cst.custo_real        
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = DEZ_2022 )
     where  rec.dt_mes_base = DEZ_2022
    ) rec
left join nov_2022 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo 
     and rec.dt_mes_base = DEZ_2022 )
where  rec.dt_mes_base = DEZ_2022
order by rec.dt_mes_base
)


, jan_2023 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        0 as valor_recebido_prev,
        0 as custo_prev,
        rec.custo_real, 
        sald.saldo saldo_inicial, 
        0 as saldo_prev,
        rec.valor_recebido-rec.custo_real + if(sald.saldo is null, 0, sald.saldo) saldo_real, 
        FEV_2023 dt_mes_base_saldo 
    from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           cst.custo_prev,
           cst.custo_real 
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = JAN_2023 )
     where  rec.dt_mes_base = JAN_2023
    ) rec
left join saldo sald  
  on (rec.dt_mes_base = sald.dt_mes_base 
     and sald.dt_mes_base = JAN_2023 )
where  rec.dt_mes_base = JAN_2023
order by rec.dt_mes_base
)


, fev_2023 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_recebido_prev,
        rec.custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        0 AS saldo_prev, 
        rec.valor_recebido-rec.custo_real + if(sald.saldo_real is null, 0, sald.saldo_real) saldo_real, 
        MAR_2023 dt_mes_base_saldo 
    from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           rec.valor_recebido_prev,
           cst.custo_prev,
           cst.custo_real 
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = FEV_2023 )
     where  rec.dt_mes_base = FEV_2023
    ) rec
left join jan_2023 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo 
     and rec.dt_mes_base = FEV_2023 )
where  rec.dt_mes_base = FEV_2023
order by rec.dt_mes_base
)

, mar_2023 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_recebido_prev,
        rec.custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        0 saldo_prev, 
        rec.valor_recebido-rec.custo_real + if(sald.saldo_real is null, 0, sald.saldo_real) saldo_real, 
        ABR_2023 dt_mes_base_saldo 
    from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           rec.valor_recebido_prev,
           cst.custo_prev,
           cst.custo_real 
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = MAR_2023 )
     where  rec.dt_mes_base = MAR_2023
    ) rec
left join fev_2023 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo 
     and rec.dt_mes_base = MAR_2023 )
where  rec.dt_mes_base = MAR_2023
order by rec.dt_mes_base
)

, abr_2023 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_recebido_prev,
        rec.custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        0 saldo_prev, 
        rec.valor_recebido-rec.custo_real + if(sald.saldo_real is null, 0, sald.saldo_real) saldo_real, 
        MAI_2023 dt_mes_base_saldo 
    from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           rec.valor_recebido_prev,
           cst.custo_prev,
           cst.custo_real 
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = ABR_2023 )
     where  rec.dt_mes_base = ABR_2023
    ) rec
left join mar_2023 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo 
     and rec.dt_mes_base = ABR_2023 )
where  rec.dt_mes_base = ABR_2023
order by rec.dt_mes_base
)

, mai_2023 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_recebido_prev,
        rec.custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        0 saldo_prev, 
        rec.valor_recebido-rec.custo_real + if(sald.saldo_real is null, 0, sald.saldo_real) saldo_real, 
        JUN_2023 dt_mes_base_saldo 
    from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           rec.valor_recebido_prev,
           cst.custo_prev,
           cst.custo_real 
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base =  MAI_2023 )
     where  rec.dt_mes_base = MAI_2023
    ) rec
left join abr_2023 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo 
     and rec.dt_mes_base = MAI_2023 )
where  rec.dt_mes_base = MAI_2023
order by rec.dt_mes_base
)

, jun_2023 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_recebido_prev,
        rec.custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        0 saldo_prev, 
        rec.valor_recebido-rec.custo_real + if(sald.saldo_real is null, 0, sald.saldo_real) saldo_real, 
        JUL_2023 dt_mes_base_saldo 
    from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           rec.valor_recebido_prev,
           cst.custo_prev,
           cst.custo_real 
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base =  JUN_2023 )
     where  rec.dt_mes_base = JUN_2023
    ) rec
left join mai_2023 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo 
     and rec.dt_mes_base = JUN_2023 )
where  rec.dt_mes_base = JUN_2023
order by rec.dt_mes_base
)

, jul_2023 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_recebido_prev,
        rec.custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        rec.valor_recebido_prev-(rec.custo_prev + rec.custo_real) + if(sald.saldo_real is null, 0, sald.saldo_real) saldo_prev, 
        rec.valor_recebido-rec.custo_real + if(sald.saldo_real is null, 0, sald.saldo_real) saldo_real, 
        AGO_2023 dt_mes_base_saldo 
    from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           rec.valor_recebido_prev,
           cst.custo_prev,
           cst.custo_real 
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base =  JUL_2023 )
     where  rec.dt_mes_base = JUL_2023
    ) rec
left join jun_2023 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo 
     and rec.dt_mes_base = JUL_2023 )
where  rec.dt_mes_base = JUL_2023
order by rec.dt_mes_base
)

, ago_2023 as (
select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_recebido_prev,
        rec.custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        rec.valor_recebido_prev-rec.custo_prev + if(sald.saldo_real is null, 0, sald.saldo_real) saldo_prev, 
        rec.valor_recebido-rec.custo_real + if(sald.saldo_real is null, 0, sald.saldo_real) saldo_real, 
        SET_2023 dt_mes_base_saldo 
    from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           rec.valor_recebido_prev,
           cst.custo_prev,
           cst.custo_real 
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base =  AGO_2023 )
     where  rec.dt_mes_base = AGO_2023
    ) rec
left join jul_2023 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo 
     and rec.dt_mes_base = AGO_2023 )
where  rec.dt_mes_base = AGO_2023
order by rec.dt_mes_base
)


select * from (

select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real, saldo_prev, saldo_real from jan_2021
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from fev_2021
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from mar_2021
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from abr_2021
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from mai_2021
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,   saldo_prev, saldo_real from jun_2021
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from jul_2021
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from ago_2021
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from set_2021
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from out_2021
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from nov_2021
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from dez_2021
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from jan_2022
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from fev_2022
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from mar_2022
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from abr_2022
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from mai_2022
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from jun_2022
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from jul_2022
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from ago_2022
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from set_2022
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from out_2022
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from nov_2022
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from dez_2022
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from jan_2023
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from fev_2023
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from mar_2023
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from abr_2023
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from mai_2023
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from jun_2023
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from jul_2023
union all 
select dt_mes_base,	valor_recebido, valor_recebido_prev,	custo_prev valor_custo_prev, custo_real valor_custo_real,  saldo_prev, saldo_real from ago_2023

)
order by dt_mes_base

;END;

/** validacao dos valores

select rec.dt_mes_base,
        rec.valor_recebido, 
        rec.valor_recebido_prev,
        rec.custo_prev,
        rec.custo_real, 
        sald.saldo_real saldo_inicial, 
        rec.valor_recebido_prev-rec.custo_prev + if(sald.saldo_prev is null, 0, sald.saldo_prev) saldo_prev, 
        rec.valor_recebido-rec.custo_real + if(sald.saldo_real is null, 0, sald.saldo_real) saldo_real,
        JUN_2022 dt_mes_base_saldo 
    from ( 
    select rec.dt_mes_base,
           rec.valor_recebido,
           rec.valor_recebido_prev,
           cst.custo_prev,
           cst.custo_real        
      from receb rec
      left join custo_base cst 
        on (rec.dt_mes_base = cst.dt_mes_base 
           and rec.dt_mes_base = MAI_2022 )
     where  rec.dt_mes_base = MAI_2022
    ) rec
left join abr_2022 sald  
  on (rec.dt_mes_base = sald.dt_mes_base_saldo 
     and rec.dt_mes_base = MAI_2022 )
where  rec.dt_mes_base = MAI_2022
order by rec.dt_mes_base


;END;

**/