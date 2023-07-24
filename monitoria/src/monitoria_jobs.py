
import pandas as pd
import pandas_gbq as pgbq
import os
import sys
import google.auth
from google.auth import impersonated_credentials
from google.oauth2 import service_account

# Construct a BigQuery client object.
from google.cloud import bigquery
from google.cloud import storage

# crdencial
def monitoria_credencial():

    import google.auth
    import os

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/home/andre/.ssh/my-chave-gcp-devsamelo2.json'

    credentials, project = google.auth.default(
        scopes = ["https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive",
                    "https://spreadsheets.google.com/feeds",
                    "https://www.googleapis.com/auth/cloud-platform",
                    "https://www.googleapis.com/auth/bigquery"]
    )

    return credentials

    
# # crdencial
# def credencial(self, sa):

#     # GERAR CHAVE PARA EXECUÇÃO DE SCRIPT SQL NO BQ NO AIRFLOW
#     target_scopes = [
#         "https://www.googleapis.com/auth/devstorage.read_only",
#         "https://www.googleapis.com/auth/drive",
#         "https://www.googleapis.com/auth/bigquery",
#         "https://www.googleapis.com/auth/cloud-platform"
#     ]

#     source_credentials, pid = google.auth.default(     
#         scopes=[
#         "https://www.googleapis.com/auth/drive",
#         "https://www.googleapis.com/auth/bigquery",
#         "https://www.googleapis.com/auth/cloud-platform"
#     ])
#     print(f"Obtained default credentials for the project {pid}")

#     credencial = impersonated_credentials.Credentials(
#     source_credentials=source_credentials,
#     #target_principal='composer-for-partners@b2w-bee-data-analytics.iam.gserviceaccount.com',
#     target_principal='bio-sheduler-jobs@composed-night-232419.iam.gserviceaccount.com',
#     target_scopes = target_scopes,
#     lifetime=500)

#     return credencial

# FUNÇÃO PARA LER SCRIPT NO STORAGE
def monitoria_get_script_sql(file):
    from google.cloud import storage

    credentials = monitoria_credencial()
    # Construct a BigQuery client object.
    storage_client = storage.Client(credentials=credentials, project='devsamelo2')
    
    # create storage client
    # get bucket with name
    bucket = storage_client.get_bucket('proj-domestico-file')
    # get bucket data as blob
    # proj = projeto
    source_file_name = f'sql/{file}'
    blob = bucket.get_blob(source_file_name)
    # convert to string
    bcontent = blob.download_as_string()
    return bcontent.decode("utf8")   

# FUNÇÃO PARA LER SCRIPT LOCAL
def get_file_local(sql):

    # substituido ler aruqivo local, nao gera o arquivo, ler no storage
    path_airflow = 'sql'
    scrp_path = f'{path_airflow}/{sql}'

    ## arquivo sql dentro da esrutura airflow: 
    f = open(scrp_path, 'r')

    conteudo = f.readlines()
    # initialize an empty string
    strsql = ""

    # converte arquivo em string
    for ln in conteudo:
        strsql += ln   

    return strsql

    # GERAR TABELA BASEADO NO DATAFRAME GERADO NO PROCESSO DA FUNÇÃO: generate_dataframe_to_information_jobs
    # GERA TABELA information_details_jobs
    # 
    def generate_table_to_dataframe(df, credencial, table_id):


        # #função que gera nome da tabela
        # project='composed-night-232419'
        # dataset='monitoria'
        # table_name='information_details_jobs'

        # table_id = project+'.'+dataset+'.'+table_name

        client = bigquery.Client('composed-night-232419',credencial)

        # TODO(developer): Set table_id to the ID of the table to create.
        #['project_id','dataset_id','table_id','row_count','last_modified_time']


        #função insert bigquery
        schema = [
            {'name': 'project_id', 'type': 'STRING', 'mode': 'nullable'},
            {'name': 'dataset_id', 'type': 'STRING', 'mode': 'nullable'},
            {'name': 'table_id', 'type': 'STRING', 'mode': 'nullable'},
            {'name': 'row_count', 'type': 'FLOAT', 'mode': 'nullable'},
            {'name': 'pre_cost_storage', 'type': 'FLOAT', 'mode': 'nullable'},
            
            {'name': 'size_tb', 'type': 'FLOAT', 'mode': 'nullable'},
            {'name': 'pre_cost_us_dol', 'type': 'FLOAT', 'mode': 'nullable'},
            {'name': 'data_ult_modificacao', 'type': 'DATE', 'mode': 'nullable'},
            {'name': 'hora_ult_modificacao', 'type': 'STRING', 'mode': 'nullable'},
            {'name': 'data_acesso', 'type': 'DATE', 'mode': 'nullable'},
            {'name': 'flag_user_sa_bio', 'type': 'BOOLEAN', 'mode': 'nullable'},
            {'name': 'flag_user_nominal', 'type': 'BOOLEAN', 'mode': 'nullable'},
            {'name': 'flag_sem_acesso', 'type': 'BOOLEAN', 'mode': 'nullable'},
            {'name': 'data_hora_execucao_job', 'type': 'DATETIME', 'mode': 'nullable'},
            {'name': 'executor_job', 'type': 'STRING', 'mode': 'nullable'},
            {'name': 'cost_us_dol', 'type': 'FLOAT', 'mode': 'nullable'},

            ]

        try:
            table = client.get_table(table_id)  # API Request
            print("Table {} already exists.".format(table_id))
        except:
            print("Table {} is not found.".format(table_id))
            # [END bigquery_table_exists]
            table = bigquery.Table(table_id, schema=schema)
            table = client.create_table(table)  # Make an API request.
            print(
                "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
            )

        job_config = bigquery.LoadJobConfig(
            # Specify a (partial) schema. All columns are always written to the
            # table. The schema is used to assist in data type definitions.

            schema = [
                # Specify the type of columns whose type cannot be auto-detected. For
                # example the "title" column uses pandas dtype "object", so its
                # data type is ambiguous.

                #project_id;dataset_id;table_id;row_count;data_ult_modificacao;hora_ult_modificacao;user_email;dt_acesso;data_carga;hora_carga;dt;executor_job;
                # total_tabela;ultima_execucao_dia;total_execucao_dia;hh;total_execucao_dia_hh

                bigquery.SchemaField("project_id", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("dataset_id", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("table_id", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("row_count", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("pre_cost_storage", bigquery.enums.SqlTypeNames.FLOAT),
                
                bigquery.SchemaField("size_tb", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("pre_cost_us_dol", bigquery.enums.SqlTypeNames.FLOAT),


                bigquery.SchemaField("data_ult_modificacao", bigquery.enums.SqlTypeNames.DATE),
                bigquery.SchemaField("hora_ult_modificacao", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("data_acesso", bigquery.enums.SqlTypeNames.DATE),
                bigquery.SchemaField("flag_user_sa_bio", bigquery.enums.SqlTypeNames.BOOLEAN),
                bigquery.SchemaField("flag_user_nominal", bigquery.enums.SqlTypeNames.BOOLEAN),
                bigquery.SchemaField("flag_sem_acesso", bigquery.enums.SqlTypeNames.BOOLEAN),
                
                bigquery.SchemaField("data_hora_execucao_job", bigquery.enums.SqlTypeNames.DATETIME),
                bigquery.SchemaField("executor_job", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("cost_us_dol", bigquery.enums.SqlTypeNames.FLOAT),
                ],

            # Optionally, set the write disposition. BigQuery appends loaded rows
            # to an existing table by default, but with WRITE_TRUNCATE write
            # disposition it replaces the table with the loaded data.
        write_disposition = "WRITE_APPEND",
        )

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)  
        # Make an API request.

        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        return "Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), table_id)


    # GERAR TABELA BASEADO NO DATAFRAME GERADO NO PROCESSO DA FUNÇÃO: generate_dataframe_to_information_jobs
    # GERA TABELA information_details_jobs
    # 
    def new_generate_table_to_dataframe(df, credencial,table_id):


        # #função que gera nome da tabela
        # project='composed-night-232419'
        # dataset='monitoria'
        # table_name='information_details_jobs'

        # table_id = project+'.'+dataset+'.'+table_name

        client = bigquery.Client('composed-night-232419',credencial)

        # TODO(developer): Set table_id to the ID of the table to create.
        #['project_id','dataset_id','table_id','row_count','last_modified_time']


        #função insert bigquery
        schema = [
            {'name': 'project_id', 'type': 'STRING', 'mode': 'nullable'},
            {'name': 'dataset_id', 'type': 'STRING', 'mode': 'nullable'},
            {'name': 'table_id', 'type': 'STRING', 'mode': 'nullable'},
            {'name': 'row_count', 'type': 'FLOAT', 'mode': 'nullable'},
            {'name': 'pre_cost_storage', 'type': 'FLOAT', 'mode': 'nullable'},
            
            {'name': 'size_tb', 'type': 'FLOAT', 'mode': 'nullable'},
            {'name': 'pre_cost_us_dol', 'type': 'FLOAT', 'mode': 'nullable'},
            {'name': 'data_ult_modificacao', 'type': 'DATE', 'mode': 'nullable'},
            {'name': 'hora_ult_modificacao', 'type': 'STRING', 'mode': 'nullable'},
            {'name': 'data_acesso', 'type': 'DATE', 'mode': 'nullable'},
            {'name': 'flag_user_sa_bio', 'type': 'BOOLEAN', 'mode': 'nullable'},
            {'name': 'flag_user_nominal', 'type': 'BOOLEAN', 'mode': 'nullable'},
            {'name': 'flag_sem_acesso', 'type': 'BOOLEAN', 'mode': 'nullable'},
            {'name': 'data_hora_execucao_job', 'type': 'DATETIME', 'mode': 'nullable'},
            {'name': 'executor_job', 'type': 'STRING', 'mode': 'nullable'},
            {'name': 'cost_us_dol', 'type': 'FLOAT', 'mode': 'nullable'},

            ]

        try:
            table = client.get_table(table_id)  # API Request
            print("Table {} already exists.".format(table_id))
        except:
            print("Table {} is not found.".format(table_id))
            # [END bigquery_table_exists]
            table = bigquery.Table(table_id, schema=schema)
            table = client.create_table(table)  # Make an API request.
            print(
                "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
            )

        job_config = bigquery.LoadJobConfig(
            # Specify a (partial) schema. All columns are always written to the
            # table. The schema is used to assist in data type definitions.

            schema = [
                # Specify the type of columns whose type cannot be auto-detected. For
                # example the "title" column uses pandas dtype "object", so its
                # data type is ambiguous.

                #project_id;dataset_id;table_id;row_count;data_ult_modificacao;hora_ult_modificacao;user_email;dt_acesso;data_carga;hora_carga;dt;executor_job;
                # total_tabela;ultima_execucao_dia;total_execucao_dia;hh;total_execucao_dia_hh

                bigquery.SchemaField("project_id", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("dataset_id", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("table_id", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("row_count", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("pre_cost_storage", bigquery.enums.SqlTypeNames.FLOAT),
                
                bigquery.SchemaField("size_tb", bigquery.enums.SqlTypeNames.FLOAT),
                bigquery.SchemaField("pre_cost_us_dol", bigquery.enums.SqlTypeNames.FLOAT),


                bigquery.SchemaField("data_ult_modificacao", bigquery.enums.SqlTypeNames.DATE),
                bigquery.SchemaField("hora_ult_modificacao", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("data_acesso", bigquery.enums.SqlTypeNames.DATE),
                bigquery.SchemaField("flag_user_sa_bio", bigquery.enums.SqlTypeNames.BOOLEAN),
                bigquery.SchemaField("flag_user_nominal", bigquery.enums.SqlTypeNames.BOOLEAN),
                bigquery.SchemaField("flag_sem_acesso", bigquery.enums.SqlTypeNames.BOOLEAN),
                
                bigquery.SchemaField("data_hora_execucao_job", bigquery.enums.SqlTypeNames.DATETIME),
                bigquery.SchemaField("executor_job", bigquery.enums.SqlTypeNames.STRING),
                bigquery.SchemaField("cost_us_dol", bigquery.enums.SqlTypeNames.FLOAT),
                ],

            # Optionally, set the write disposition. BigQuery appends loaded rows
            # to an existing table by default, but with WRITE_TRUNCATE write
            # disposition it replaces the table with the loaded data.
        write_disposition = "WRITE_APPEND",
        )

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)  
        # Make an API request.

        job.result()  # Wait for the job to complete.

        table = client.get_table(table_id)  # Make an API request.
        return "Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), table_id)
    
    # GERAR DATAFRAME BASEADO NO SCRIPT
    # 
    def generate_dataframe_to_script(self, projeto, sql, param, value, credencial):

        import pandas_gbq as pgbq

        monitoria = MonitoriaJobs()

        strsql = monitoria.get_file_local(sql)    

        if projeto=='frete-prd':
            project_id='b2w-bee-u-b2w-frete-prd'    

        elif projeto=='bio-stg':
            project_id='composed-night-232419'   


        if param=='dataset':
            strsql = strsql%{"dataset": value}
            df = pgbq.read_gbq(strsql, project_id=project_id , credentials=credencial)
        elif param=='table_id':
            strsql = strsql%{"table_id": value}
            df = pgbq.read_gbq(strsql, project_id=project_id , credentials=credencial)            
        elif param==None:
            df = pgbq.read_gbq(strsql, project_id=project_id, credentials=credencial)    

        return df

    # EXECUTAR SCRIPT NO BQ(CREATE, MERGE)
    #
    def generate_table_to_script(self, projeto, sql, param, value, credencial):
        #create_information_executor_jobs


        # Construct a BigQuery client object.
        from google.cloud import bigquery

        if projeto=='frete-prd':
            project_id='b2w-bee-u-b2w-frete-prd'    

        elif projeto=='bio-stg':
            project_id='composed-night-232419'  

        client = bigquery.Client('composed-night-232419',credencial)

        monitoria = MonitoriaJobs()
        strsql = monitoria.get_file_local(sql)   

        import re
        result = re.findall(r'(\w+).sql',sql)  
        prefix = ''.join(result)   

        if param=='dias':
            strsql = strsql%{"dias": value}
        elif param==None:
            strsql = strsql
        
        query_job = client.query(
        strsql,
        # Explicitly force job execution to be routed to a specific processing
        # location.
        location="US",
        # Specify a job configuration to set optional job resource properties.
        job_config=bigquery.QueryJobConfig(
            labels={"information_job": "bee_analytics_transporte"}
        ),
        # The client libraries automatically generate a job ID. Override the
        # generated ID with either the job_id_prefix or job_id parameters.
        job_id_prefix=f"bee_analytics_transp_{prefix}",
        )  # Make an API request.

        print("Started job: {}".format(query_job.job_id))

    #-> FASE INICAL DE PRE PROCESSMENTO DO MONITORAMENTO DE TABELAS


    # INFORMAÇÕES DOS JOBS EXECUTADOS COM PARAMETRO DE DIAS RETROATIVOS
    # QUERY USA COMO BASE: #  composed-night-232419.INFORMATION_SCHEMA.SCHEMATA
    # PRIMEIRA QUERY EXECUTADA PARA GERAR UM BASE PARA PROXIMO PROCESSO
    # 
    def generate_table_to_information_executor_jobs(self, projeto, credencial):

        monitoria = MonitoriaJobs() 
        sql = ""

        sql = f'{projeto}/information_jobs_geral/create_information_executor_jobs.sql'
        param='dias'
        monitoria.generate_table_to_script(projeto, sql, param, 10, credencial)        

    # INFORMAÇÕES QUE RETORNAM UM DATAFRAME COM DADOS DAS TABELAS DO DATASET
    # composed-night-232419.INFORMATION_SCHEMA.SCHEMATA
    # DADOS DE USUARIOS: composed-night-232419.bee_analytics_transporte.table_users
    # DADOS DE STORAGE E CUSTO DE EXECUÇÃO DE JOBS
    # O DATAFRAME SERA GRAVADO NUMA TABELA CHAMADA NA FUNÇÃO:generate_table_to_df_information_details_jobs
    # 
    def generate_table_to_information_jobs(self, projeto, credencial):

        from google.cloud import bigquery
        import pandas as pd
        import pandas_gbq as pgbq

        monitoria = MonitoriaJobs()

        if projeto == 'bio-stg':

            strsql= '''

            SELECT
                SCHEMA_NAME dataset
            FROM
            composed-night-232419.INFORMATION_SCHEMA.SCHEMATA;
            '''
            #executesql('sql/information_jobs.sql')
            dfdatasets = pgbq.read_gbq(strsql, project_id='composed-night-232419' , credentials=credencial)

            df_col = dfdatasets['dataset'].unique()

        elif projeto == 'frete-prd':

            df_col = ['medida_certa_pipeline','medida_certa_dashboards', 'medida_certa_resultados']
            
        
        lista_retorno_append = []

        # executa um for consulta dataset e suas tabelas e armazena num dataframe

        for index, value in enumerate(df_col, start=1):
            strsql = ""

            if value is not None: # and value[1] is not None:
                
                if projeto == 'bio-stg':

                    client = bigquery.Client('composed-night-232419',credencial)
                    ret = client.get_dataset(value)
                
                elif projeto == 'frete-prd':
                    client = bigquery.Client('b2w-bee-u-b2w-frete-prd'  ,credencial)
                    ret = client.get_dataset(value)

                #print(ret.dataset_id)
                if len(ret.dataset_id):     

                    sql = f'{projeto}/information_jobs_geral/select_information_dataset.sql'
                    param = 'dataset'
                    value = value

                    df_valid = monitoria.generate_dataframe_to_script(projeto, sql, param, value, credencial)
                    
                    df_valid['data_ult_modificacao'] = df_valid['last_modified_time'].dt.strftime('%Y-%m-%d')
                    df_valid['hora_ult_modificacao'] = df_valid['last_modified_time'].dt.strftime('%H')
                    
                    lista_retorno_append = df_valid[['project_id','dataset_id','table_id','row_count','pre_cost_storage','size_tb','pre_cost_us_dol','data_ult_modificacao','hora_ult_modificacao']].values.tolist()
                    if index==1:
                        df_info = pd.DataFrame(lista_retorno_append, columns=['project_id','dataset_id','table_id','row_count','pre_cost_storage','size_tb','pre_cost_us_dol','data_ult_modificacao','hora_ult_modificacao'])
                    else:
                        df_info = df_info.append(pd.DataFrame(lista_retorno_append, columns=['project_id','dataset_id','table_id','row_count','pre_cost_storage','size_tb','pre_cost_us_dol','data_ult_modificacao','hora_ult_modificacao']))


        # gera uma lista de todas as tabelas geradas no df dos datasets
        list_tab = [str(l) for l in df_info['table_id'].tolist()]

        # gerar novo dataframe com dados usuarios das tabelas listadas
        sql = f'{projeto}/information_jobs_geral/select_information_user_consumers.sql'
        value=str(list_tab)[1:-1]
        param='table_id'
        df_user = monitoria.generate_dataframe_to_script(projeto, sql, param, value, credencial)

        df_user.rename(columns={"access_date":"data_acesso"}, inplace=True)
        df_user['data_acesso'] = pd.to_datetime(df_user['data_acesso'], format="%Y-%m-%d")

        df_user = df_user[['project_id','table_id','dataset_id','data_acesso', 'flag_user_sa_bio', 'flag_user_nominal', 'flag_sem_acesso']]
        
        # gerar novo dataframe com dados usuarios das tabelas listadas
        df_info2 = pd.merge(df_info, df_user, how="left", on=["project_id","table_id","dataset_id"])

        df_info2['data_ult_modificacao'] = df_info2['data_ult_modificacao'].astype(str)
        df_info2['data_ult_modificacao'] = pd.to_datetime(df_info2['data_ult_modificacao'], format="%Y-%m-%d")

        # gerar novo dataframe com dados das execuções dos jobs
        sql = f'{projeto}/information_jobs_geral/select_information_executor_jobs.sql'
        df_tabgcp = monitoria.generate_dataframe_to_script(projeto, sql, None, None, credencial)

        df_tabgcp['data_hora_execucao_job'] = pd.to_datetime(df_tabgcp['data_hora_execucao_job'], format="%Y-%m-%d %H:%M:%S") #.dt.strftime('%Y-%m-%d')
        df_tabgcp.rename(columns={"projeto":"project_id","tabela":"table_id","dataset":"dataset_id"}, inplace=True)

        # df de todas as tabelas e realiza merge de info de users e execuções de jobs
        df_info3 = pd.merge( df_info2, df_tabgcp, how="left", on=["project_id","table_id","dataset_id"])

        monitoria.generate_table_to_dataframe(df_info3, credencial)

    #-> FASE FINAL DO PROCESSO DE MONITORAMENTO DE TABELAS

    # GERAR TABELA FONTE DAS INFORMAÇÕES DA TABELA information_details_jobs
    # script - merge_information_cust_acess_update_tables.sql: REALIZA MERGE NA TABELA historico_info_tabelas
    def generate_table_to_historico_info_tabelas(self, projeto, credencial):
        import time
        monitoria = MonitoriaJobs() 

        for dias in range(-1,4):

            wait_time = 10
            time.sleep(wait_time)

            sql = ""
            sql = f'{projeto}/information_analitico/merge_historico_info_tabelas.sql'
            param='dias'
            monitoria.generate_table_to_script(projeto, sql, param, dias, credencial)
        
    # GERAR TABELA DE APOIO ANALITICA FONTE DAS INFORMAÇÕES DA TABELA: historico_info_tabelas
    # script - create_analitico_info_tabelas.sql: REALIZA CREATE DA TABELA consolidado_info_tabelas
    def generate_table_to_consolidado_info_tabelas(self, projeto, credencial):

        monitoria = MonitoriaJobs() 
        sql = ""
        # buscar no storage: gs://us-east1-bee-transport-crm--a130093f-bucket/dags/transporte/scripts
        # projeto 
        # sql = f'{path}/create_acompanhamento_tabelas_atuais.sql'
        # strsql = monitoria.get_file_bucket(sql, credencial)

        #substituir metodo busca sql local: '/home/airflow/dags/transporte/scripts'
        sql = f'{projeto}/information_analitico/create_consolidado_info_tabelas.sql'
        monitoria.generate_table_to_script(projeto, sql, None, None, credencial)

    # GERAR TABELA FINAL ANALITICA FONTE DAS INFORMAÇÕES DA TABELA: consolidado_info_tabelas
    # script - merge_analitico_tabelas_consolidado.sql: REALIZA MERGE CONSOLIDA DADOS DIARIOS DA TABELA monitoria_tabelas_ativas
    def genarate_table_to_monitoria_tabelas_ativas(self, projeto, credencial):

        import time
        monitoria = MonitoriaJobs() 

        for dias in range(-1,4):

            wait_time = 10
            time.sleep(wait_time)

            sql = ""
            #substituir metodo busca sql local: '/home/airflow/dags/transporte/scripts'
            sql = f'{projeto}/information_analitico/merge_monitoria_tabelas_ativas.sql'
            param='dias'
            monitoria.generate_table_to_script(projeto, sql, param, dias, credencial)

## chamada do metodo do projeto monitoria em python no airflow
def monitoria_analitica(params):


    monitoria = MonitoriaJobs() 
    # INICIO DO PROCESSO DA MONITORIA ANALITICA
    # print(params)

    #projeto = params['projeto']
    projeto = params

    #-> FASE INICAL DE PRE PROCESSMENTO DO MONITORAMENTO DE TABELAS

    # INICIO DE GERAR INFORMAÇÕES DE TABELAS , DATASET, JOBS E USERS E ARMAZENAR EM DATAFRAME
    monitoria.generate_table_to_information_executor_jobs(projeto, monitoria.credencial_local(None))
    
    # GERA UM DATAFRAME COM INFORMAÇÕES DE 10 DIAS DE JOBS E CONSOLIDA NA TABELA 
    monitoria.generate_table_to_information_jobs(projeto, monitoria.credencial_local(None))

    #-> FASE FINAL DO PROCESSO DE MONITORAMENTO DE TABELAS

    # GERAR TABELA FONTE DAS INFORMAÇÕES NA TABELA information_details_jobs
    # script - merge_information_cust_acess_update_tables.sql: REALIZA MERGE NA TABELA information_cust_acess_update_tables
    monitoria.generate_table_to_historico_info_tabelas(projeto, monitoria.credencial_local(None))

    # GERAR TABELA DE APOIO ANALITICA FONTE DAS INFORMAÇÕES NA TABELA: information_cust_acess_update_tables
    # script - create_analitico_info_tabelas.sql: REALIZA CREATE DA TABELA acompanhamento_tabelas_atuais
    monitoria.generate_table_to_consolidado_info_tabelas(projeto, monitoria.credencial_local(None))
    
    # GERAR TABELA FINAL ANALITICA FONTE DAS INFORMAÇÕES NA TABELA: acompanhamento_tabelas_atuais
    # script - merge_analitico_tabelas_consolidado.sql: REALIZA MERGE CONSOLIDA DADOS DIARIOS DA TABELA information_acompanhamento_tabelas_atuais
    monitoria.genarate_table_to_monitoria_tabelas_ativas(projeto, monitoria.credencial_local(None))

if __name__ == "__main__":
    monitoria_analitica('frete-prd')