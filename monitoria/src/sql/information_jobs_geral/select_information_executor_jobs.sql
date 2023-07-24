--> jobs gerados pelo script infomation_job.sql

select 
    project_id
    , dataset_id_job dataset_id
    , table_id
    , start_time data_hora_execucao_job
    , executor_job
    , cost_us_dol
  from composed-night-232419.bee_analytics_transporte.information_executor_jobs
 where statement_type not in ('DELETE')
    and project_id = 'composed-night-232419'
    and dataset_id_job is not null
    and table_id is not null