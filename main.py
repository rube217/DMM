from distutils.log import debug
import services, sqlite3, datetime, logging, database
print()
logging.basicConfig(filename='\logs\MMRepoMain.log', encoding='utf-8', level=logging.DEBUG)

services.create_database()

exporter_list = [{'Machine':'PV10219',
                    'root': r'\\10.240.160.176\f$\Network Exporter Invoker\Log\NetworkExporterExecutionService.log'}, 
                {'Machine':'PV10270',
                    'root': r'\\10.240.160.161\e$\Network Exporter Invoker\Log\NetworkExporterExecutionService.log'}]

NE_Executions,NE_Executions_database = services.extract_NE_data(exporter_list)

services.update_NE_table(NE_Executions,NE_Executions_database)

services.insert_NE_table(NE_Executions,NE_Executions_database)

services.update_adms_tables()



logging.debug("Ejecutado a las {}".format(str(datetime.datetime.now())))

print('Finaliza')