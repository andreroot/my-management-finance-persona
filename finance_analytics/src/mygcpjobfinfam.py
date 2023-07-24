
def my_execute_job(sql):
    from mygcpcredencial import my_credencial
    from google.cloud import bigquery
    
    credentials = my_credencial()
    # Construct a BigQuery client object.
    client = bigquery.Client(credentials=credentials, project='devsamelo2')
    
    query_job = client.query(
        sql,
        # Explicitly force job execution to be routed to a specific processing
        # location.
        location="US",
        # Specify a job configuration to set optional job resource properties.
        job_config=bigquery.QueryJobConfig(
            labels={"analise": "custo_domestico"}
        ),
        # The client libraries automatically generate a job ID. Override the
        # generated ID with either the job_id_prefix or job_id parameters.
        job_id_prefix="custo_domestico_",
    )  # Make an API request.

    print("Started job: {}".format(query_job.job_id))
    # [END bigquery_create_job]
    return query_job     
   


    #gerar tabela sheet:https://docs.google.com/spreadsheets/d/165LxRPISVoidWCXTPw7cCKnMAOO0p0zzNvJshET_5Qk/edit?resourcekey#gid=2013346928
    #CUSTO
    #data_base_bq:DATE,custo:STRING,tipo_custo:STRING,dt_custo_bq:DATE,valor_custo:FLOAT,ano_base:INTEGER,mes_base_ordem:INTEGER,mes_base:STRING