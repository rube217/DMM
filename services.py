import sqlalchemy.orm as orm, pandas as pd, sqlalchemy as sql, time, datetime as dt, re, numpy
import database, models, cx_Oracle


##cx_Oracle.init_oracle_client(lib_dir=r"F:/oracle/product/BIN/")

def create_database():
    return database.Base.metadata.create_all(bind=database.engine_datawarehouse)

def get_db():
    db = database.SessionLocal()
    try:
        yield db
    finally:
        db.close()

def extract_NE_data(exporter_list, conn = database.engine_datawarehouse):    ## Función que lee el archivo y la base de datos y devuelve 2 dataframes con la información procesada
    try:

        NE_Invoker = pd.DataFrame([],columns={0,'Machine'})
        for i in exporter_list:
            NE_Invoker = pd.concat([NE_Invoker, pd.read_csv(i['root'], sep = '\r', header=None)])
            NE_Invoker['Machine'] = NE_Invoker['Machine'].fillna(i['Machine'])
        
        NE_Invoker_Pending =  NE_Invoker[NE_Invoker[0].str.contains('ColaMensajes.Encolar')].reset_index(drop=True)
        NE_Invoker_Executed =  NE_Invoker[NE_Invoker[0].str.contains('Se esta realizando la siguiente llamada')].reset_index(drop=True)

        del(NE_Invoker)

        NE_Invoker_Pending['Datetime'] = NE_Invoker_Pending[0].str.split('.',expand=True)[0]
        NE_Invoker_Pending[['Version','KindExecution','FeederList']] = NE_Invoker_Pending[0].str.split('Version:|kindExportation:|listFeeders:',expand=True)[[1,2,3]]
        NE_Invoker_Pending.KindExecution = NE_Invoker_Pending.KindExecution.str.split(',',expand = True)[0]
        NE_Invoker_Pending.Version = NE_Invoker_Pending.Version.str[:-2]
        NE_Invoker_Pending.Datetime = NE_Invoker_Pending.Datetime.replace({'a':'AM','p':'PM'},regex=True)
        NE_Invoker_Pending.Datetime = list(map(lambda x: dt.datetime.strptime(x,'%d/%m/%Y %I:%M:%S %p'), NE_Invoker_Pending.Datetime))

        NE_Invoker_Pending = NE_Invoker_Pending.drop(columns = 0).sort_values(by='Datetime').reset_index(drop=True)
        NE_Invoker_Pending['Id'] =  NE_Invoker_Pending.apply(lambda x: format(int(x.Datetime.timestamp())*10000 + x.name),axis=1)
        NE_Invoker_Pending['GisJobId'] =  NE_Invoker_Pending['Version'].apply(lambda x: re.search('20\d+',x.split('.')[-1]).group(0) if 'GIS' in x else format(0,'011d'))

        NE_Invoker_Executed['Datetime'] = NE_Invoker_Executed[0].str.split('.',expand=True)[0]
        NE_Invoker_Executed[['Version','KindExecution','FeederList']] = NE_Invoker_Executed[0].str.split('"',expand=True)[[3,7,5]]
        NE_Invoker_Executed.Datetime = NE_Invoker_Executed.Datetime.replace({'a':'AM','p':'PM'},regex=True)
        NE_Invoker_Executed.Datetime = list(map(lambda x: dt.datetime.strptime(x,'%d/%m/%Y %I:%M:%S %p'), NE_Invoker_Executed.Datetime))
        NE_Invoker_Executed = NE_Invoker_Executed.drop(columns=0).sort_values(by='Datetime').reset_index(drop=True)
        NE_Invoker_Executed['Id'] = 0

        for i in NE_Invoker_Pending.index.sort_values(ascending=False):
            subset = NE_Invoker_Executed.loc[
                        (NE_Invoker_Executed.FeederList == NE_Invoker_Pending.loc[i,'FeederList']) & 
                        (NE_Invoker_Executed.Version== NE_Invoker_Pending.loc[i,'Version']) & 
                        (NE_Invoker_Executed.Machine== NE_Invoker_Pending.loc[i,'Machine']) &
                        (NE_Invoker_Executed.Datetime >= NE_Invoker_Pending.loc[i,'Datetime']) &
                        (NE_Invoker_Executed.Id == 0),:]
            
            NE_Invoker_Executed.loc[subset.index.min(),'Id'] = NE_Invoker_Pending.loc[i,'Id']

        NE_Executions = NE_Invoker_Pending.merge(NE_Invoker_Executed[['Id','Datetime']], on = 'Id', how='left').rename(columns={'Datetime_x':'ReceivedTime','Datetime_y':'ExecutionTime'})
        del(NE_Invoker_Executed)
        del(NE_Invoker_Pending)

        NE_Executions['Executed'] = NE_Executions['ExecutionTime'].notna()
        
        NE_Executions_db = pd.read_sql("""
            SELECT Id, GisJobId, Version, FeederList, KindExecution, ReceivedTime, ExecutionTime, Executed, Machine
            FROM Network_Exporter_Executions; """
            ,conn
            ,coerce_float=False)
        #NE_Executions_db.Executed = NE_Executions_db.Executed.fillna(False)

        return NE_Executions,NE_Executions_db
    except ValueError:
        print(ValueError)

def insert_NE_table(df_file,df_database, conn = database.engine_datawarehouse): ## Función para insertar nuevos registros en la tabla Network Exporter Executions
    try:
        last_insert = df_database[['Machine','ReceivedTime']].groupby(by='Machine').last().to_dict()

        df_dataframe = pd.DataFrame([])

        for i in last_insert['ReceivedTime']:
            df_dataframe = pd.concat([df_dataframe,df_file[(df_file.ReceivedTime>last_insert['ReceivedTime'][i])&(df_file.Machine==i)]])

        df_dataframe = df_dataframe.sort_index()
        df_dataframe[list(df_database.columns)].to_sql('Network_Exporter_Executions',con=conn, index = False, index_label='id', if_exists='append')
        return(True)
    except ValueError:
        print(ValueError)

def  update_NE_table(df_file,df_database, conn = database.engine_datawarehouse): ## Función para actualizar los tiempos de ejecución de los registros ya insertados
    try:

        df_database.ReceivedTime = pd.to_datetime(df_database.ReceivedTime)
        df_database.GisJobId = df_database.GisJobId.astype(str)

        Update = df_database[df_database.Executed==False].merge(df_file,
                                                    on=['Version','FeederList','KindExecution','ReceivedTime','Machine'],
                                                    how='inner',
                                                    suffixes=['_old',None]).rename(columns= {'Id':'Id2','Id_old':'Id'})

        Update = Update[Update.Executed!=Update.Executed_old]

        with conn.connect() as connection: 
            with connection.begin():
                metadata_NEE = sql.MetaData()

                NEE = sql.Table("Network_Exporter_Executions", metadata_NEE, autoload_with=conn)
                
                for i,x in Update.iterrows():
                    print(x.Id,x.ExecutionTime, x.Executed)
                    update_sentence = NEE.update().where(NEE.c.Id == x.Id).values(ExecutionTime=x.ExecutionTime , Executed = x.Executed)

                    connection.execute(update_sentence)
                connection.close()
                print('Finaliza update NE_table')
                return(True)
    except ValueError:
        print(ValueError)

def read_ADMS(conn=database.engine_adms):
      extract_info = pd.read_sql_query("""SELECT Id,
                  extract_alias,
                  extract_state_Id,
                  extract_source_Id,
                  description,
                  created_date as created_time,
                  last_modified_date as last_modified_time,
                  filename,
                  comment
      FROM CSRepo.dbo.EXTRACT_INFO""", conn, coerce_float=False)

      extract_state = pd.read_sql_query("""SELECT Id
            ,state_name
      FROM CSRepo.dbo.EXTRACT_STATE""", conn, coerce_float=False)

      extract_source = pd.read_sql_query("""SELECT Id
            ,source_name
      FROM CSRepo.dbo.EXTRACT_SOURCE""", conn, coerce_float=False)

      extract_hist = pd.read_sql_query("""SELECT extract_Id
            , transition_time
            , old_extract_state_Id
            , new_extract_state_Id
            , comment
            , username
      FROM CSRepo.dbo.EXTRACT_HISTORY""", conn, coerce_float=False)

      changeset_extract_dependency = pd.read_sql_query("""SELECT changeset_Id
            ,extract_Id
      FROM CSRepo.dbo.CHANGESET_EXTRACT_DEPENDENCY""", conn, coerce_float=False)

      changeset_info = pd.read_sql_query("""SELECT Id
            , changeset_type_Id
            , changeset_state_Id
            , description
            , last_modified_date
            , created_date
            , in_service
            , creator_username
      FROM CSRepo.dbo.CHANGESET_INFO""", conn, coerce_float=False)

      changeset_state = pd.read_sql_query("""SELECT state_name
            , Id FROM CSRepo.dbo.CHANGESET_STATE""",conn, coerce_float=False)

      changeset_hist = pd.read_sql_query("""SELECT changeset_Id
            , old_changeset_state_Id
            , new_changeset_state_Id
            , username
            , transition_time
            , comment
      FROM CSRepo.dbo.CHANGESET_HISTORY""", conn, coerce_float=False)

      changeset_type = pd.read_sql_query("SELECT Id, type_name FROM CSRepo.dbo.CHANGESET_TYPE",conn, coerce_float=False)

      extract_hist.reset_index(inplace=True)
      extract_hist = extract_hist.rename(columns={'index':'Id'})

      changeset_extract_dependency.reset_index(inplace=True)
      changeset_extract_dependency = changeset_extract_dependency.rename(columns={'index':'Id'})

      changeset_hist.reset_index(inplace=True)
      changeset_hist = changeset_hist.rename(columns={'index':'Id'})

      extract_info['state_name'] = extract_info.merge(extract_state,left_on='extract_state_Id', right_on='Id', how = 'left')['state_name']
      extract_info['source_name'] = extract_info.merge(extract_source,left_on='extract_source_Id', right_on='Id', how = 'left')['source_name']
      changeset_info['current_state'] = changeset_info.merge(changeset_state,left_on='changeset_state_Id', right_on='Id', how = 'left')['state_name']
      changeset_info['changeset_type'] = changeset_info.merge(changeset_type,left_on='changeset_type_Id', right_on='Id', how = 'left')['type_name']
      extract_hist['old_state'] = extract_hist.merge(extract_state, left_on='old_extract_state_Id', right_on='Id', how='left')['state_name']
      extract_hist['new_state'] = extract_hist.merge(extract_state, left_on='new_extract_state_Id', right_on='Id', how='left')['state_name']
      changeset_hist['old_state'] = changeset_hist.merge(changeset_state, left_on='old_changeset_state_Id', right_on='Id', how='left')['state_name']
      changeset_hist['new_state'] = changeset_hist.merge(changeset_state, left_on='new_changeset_state_Id', right_on='Id', how='left')['state_name']
      

      return{'Extracts': extract_info.drop(columns=['extract_state_Id','extract_source_Id']),
             'Extracts_History': extract_hist.drop(columns=['old_extract_state_Id','new_extract_state_Id']),
             'Changesets': changeset_info.drop(columns=['changeset_state_Id','changeset_type_Id']),
             'Changesets_History':changeset_hist.drop(columns=['old_changeset_state_Id', 'new_changeset_state_Id']),
             'Extracts_Changesets_Dependency' : changeset_extract_dependency}

def update_adms_tables(engine = database.engine_datawarehouse,adms_df = read_ADMS()):
    with engine.connect() as connection:
        with connection.begin():
            for i in adms_df:
                connection.execute('DELETE FROM {};'.format(i))
                print('Actualizando',i)
                adms_df[i].to_sql(i, index=False,index_label='Id', if_exists='append', con=connection)
        connection.close()

def update_gis_jobs(df_file,df_databse,engine_datawarehouse = database.engine_datawarehouse, engine_gis = database.engine_gis):
    gis_jobs_NE = list(pd.concat([df_databse.GisJobId, df_file.GisJobId]).unique())
    splits = numpy.array_split(gis_jobs_NE, round(len(gis_jobs_NE)/900))


    with engine_datawarehouse.connect() as connection:

            with connection.begin():
                print('Borrando Gis_Jobs')
                connection.execute('DELETE FROM GIS_Jobs;')
                print('Actualizando Gis_Jobs')

            connection.close()
    for i in range(len(splits)):
        gis_jobs = pd.read_sql("""
            SELECT DISTINCT jj.JOB_NAME as JobName, JJSX.job_id as Id ,  v.owner || '.' || v.name AS Version, jj.DESCRIPTION, jjs.STEP_NAME as CurrentStatus, '' as Notes, jj.OWNED_BY as Owner 
                FROM GRED_ADMINIS.JTX_STEP_STATUS jjsx 
                LEFT JOIN GRED_ADMINIS.JTX_JOB_STEP jjs ON jjs.STEP_ID  = JJSX.step_id
                LEFT JOIN GRED_ADMINIS.JTX_JOBS jj ON jj.JOB_ID = jjsx.JOB_ID 
                LEFT JOIN sde.versions v ON v.name LIKE '%'|| jj.JOB_NAME ||'%' 
                WHERE jjsx.STATUS = 'S'
                and jj.JOB_NAME IN {} 
                """.format(tuple(splits[i])), engine_gis)
        gis_jobs = gis_jobs[-(gis_jobs.id.duplicated(keep='first'))]
        gis_jobs.to_sql("GIS_Jobs", con=engine_datawarehouse, index=False, index_label='JobName',if_exists='append')
        print(i)

    print('Gis Jobs Actualizado')