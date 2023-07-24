def get_file_local(sql):
    import os

    # substituido ler aruqivo local, nao gera o arquivo, ler no storage
    path_ = os.getcwd()
    scrp_path = f'{path_}/sql/{sql}'

    ## arquivo sql dentro da esrutura airflow: 
    f = open(scrp_path, 'r')

    conteudo = f.readlines()
    # initialize an empty string
    strsql = ""

    # converte arquivo em string
    for ln in conteudo:
        strsql += ln   

    return strsql

def execute_query_2022():

    from mygcpjobfinfam import my_execute_job
    exec_job = my_execute_job


    lista = ['tab_estrat_custo_consolidado_2022.sql', 'tab_credito_2022.sql', 'tab_recebimento_geral_2022.sql', 'tab_saldo_2022.sql', 'tab_custo_vida_geral_2022.sql']
    for f in lista:
        sql = get_file_local(f)

        import time
        wait_time = 10
        time.sleep(wait_time)
        
        exec_job(sql)

if __name__=='__main__':

    execute_query_2022()