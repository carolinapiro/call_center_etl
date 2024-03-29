
import pandas as pd
from datetime import datetime, timedelta
import sys

from data_connectors.dm_connector import DMConnector


class DM_Processes:

    def __init__(self):
        self.table = "dm_processes"
        self.dm_connector = DMConnector()


    def buscar_procesos_pendientes(self):
        try:
            return pd.read_sql("SELECT * FROM " + self.table +
                            " WHERE status = 'pendiente'; ",
                            self.dm_connector.engine)
        except:
            error_message = 'Error - ' + str(sys.exc_info()[1])
            print(error_message)

    def insertar_proceso_diario(self):
        fecha_ayer = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        fecha_hoy = (datetime.today()).strftime('%Y-%m-%d')

        self.dm_connector.execute_query(query="insert into dm_processes (fact_table, " + 
                                                                        "start_date, " +
                                                                        "end_date, " +
                                                                        "status) " +
                                               "VALUES ('fact_llamadas', " +
                                               " '" + fecha_ayer + "', " +
                                               " '" + fecha_hoy + "', " +
                                               " 'pendiente' );")


    def actualizar_estado_proceso_corriendo(self, id_process: int):

        self.dm_connector.execute_query(query="UPDATE dm_processes " + 
                                              "SET status = 'corriendo', " +
                                                  "process_run_date = '" + str(datetime.today()) + "' " +
                                              "WHERE id_process = " + str(id_process) )
        

    def actualizar_estado_proceso_exitoso(self, id_process: int):

        self.dm_connector.execute_query(query="UPDATE dm_processes " + 
                                              "SET status = 'exitoso' " +
                                              "WHERE id_process = " + str(id_process) )