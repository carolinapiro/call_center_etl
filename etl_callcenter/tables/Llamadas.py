import pandas as pd

from data_connectors.dm_connector import DMConnector
from tables.BaseTables import FactTable

from datetime import datetime


class Llamadas(FactTable):
    
    def __init__(self, start_date: datetime, end_date: datetime):
        self.stg_table = "stg_fact_llamadas"
        self.table = "fact_llamadas"
        self.id = "id_llamada"

        self.source_data_type = 'csv'
        self.source_data_path = 'source_files/Llamadas_2024.csv'

        self.start_date = start_date
        self.end_date = end_date
        self.date_field = 'Fecha'

        self.dm_connector = None
        self.source_connector = None

        FactTable.__init__(self)


    def limpiar_datos_fuente(self):
        self.source_data_df_clean = self.source_data_df

        # Change data types - for all fields
        self.source_data_df_clean['Fecha'] = pd.to_datetime(self.source_data_df_clean['Fecha'])
        self.source_data_df_clean['IDCliente'] = self.source_data_df_clean['IDCliente'].astype('int')
        self.source_data_df_clean['Campania'] = self.source_data_df_clean['Campania'].astype('string')
        self.source_data_df_clean['Telefono'] = self.source_data_df_clean['Telefono'].astype('string')
        self.source_data_df_clean['Agente'] = self.source_data_df_clean['Agente'].astype('string')
        self.source_data_df_clean['Tiempohablado'] = self.source_data_df_clean['Tiempohablado'].astype('string')
        self.source_data_df_clean['Tiempototal'] = self.source_data_df_clean['Tiempototal'].astype('string')

        # Replace null values - for not pk or fk fields 
        self.source_data_df_clean.fillna({'Campania':'unknown'}, inplace=True)
        self.source_data_df_clean.fillna({'Telefono':'unknown'}, inplace=True)
        self.source_data_df_clean.fillna({'Tiempohablado': '00:00:00' }, inplace=True)
        self.source_data_df_clean.fillna({'Tiempototal': '00:00:00' }, inplace=True)

        # Rename fields - for all fields
        self.source_data_df_clean.rename(columns = {'Fecha':'fecha_llamada'}, inplace = True) 
        self.source_data_df_clean.rename(columns = {'IDCliente':'id_cliente_llamada'}, inplace = True) 
        self.source_data_df_clean.rename(columns = {'Campania':'campania_llamada'}, inplace = True) 
        self.source_data_df_clean.rename(columns = {'Telefono':'telefono_llamada'}, inplace = True) 
        self.source_data_df_clean.rename(columns = {'Agente':'agente_llamada'}, inplace = True) 
        self.source_data_df_clean.rename(columns = {'Tiempohablado':'tiempo_hablado_llamada'}, inplace = True) 
        self.source_data_df_clean.rename(columns = {'Tiempototal':'tiempo_total_llamada'}, inplace = True) 


    def cargar_tabla_stg(self):
        self.dm_connector.append_data_to_table(table=self.stg_table, data=self.source_data_df_clean)
    
        self.dm_connector.execute_query(query="UPDATE " + self.stg_table +
                                              " SET " + "id_agente_llamada" + " = dim_agentes_id_lookup(agente_llamada) ")


    def validar_dimensiones_fk(self):
        
        # Replace Null ids with default member's key
        self.dm_connector.execute_query(query="UPDATE " + self.stg_table +
                                              " SET " + "id_cliente_llamada" + " = -999 " + 
                                              " WHERE " + "id_cliente_llamada" + " is null ") 
        
        self.dm_connector.execute_query(query="UPDATE " + self.stg_table +
                                              " SET " + "id_agente_llamada" + " = -999 " + 
                                              " WHERE " + "id_agente_llamada" + " is null ")

        # Add inexistent ids to related dim table
        self.dm_connector.execute_query(query="DROP TABLE IF EXISTS " + self.stg_table + "_new_client_ids; " + 
                                              "CREATE TEMPORARY TABLE " + self.stg_table + "_new_client_ids " + " AS" +
                                              " SELECT DISTINCT " + "id_cliente_llamada" +
                                              " FROM " + self.stg_table +  
                                              " WHERE " + "id_cliente_llamada" + " NOT IN ("
                                                                                    "SELECT DISTINCT " + "id_cliente" +
                                                                                    " FROM " + "dim_clientes" +
                                                                                "); " +
                                              "INSERT INTO " + "dim_clientes" + "( id_cliente, " + 
                                                                                  "genero_cliente, " + 
                                                                                  "tipo_cliente ) " +
                                              "SELECT id_cliente_llamada, " + 
                                                     "'unknown', " +
                                                     "'unknown'" +
                                              " FROM " + self.stg_table + "_new_client_ids; "
                                        )

        self.dm_connector.execute_query(query="DROP TABLE IF EXISTS " + self.stg_table + "_new_agentes_ids; " + 
                                              "CREATE TEMPORARY TABLE " + self.stg_table + "_new_agentes_ids " + " AS" +
                                              " SELECT DISTINCT " + "id_agente_llamada" +
                                              " FROM " + self.stg_table +  
                                              " WHERE " + "id_agente_llamada" + " NOT IN ("
                                                                                    "SELECT DISTINCT " + "id_agente" +
                                                                                    " FROM " + "dim_agentes" +
                                                                                "); " + 
                                             "INSERT INTO " + "dim_agentes" + "( id_agente, " + 
                                                                                "agente, " +
                                                                                "nivel_expertise_agente, " +
                                                                                "nombre_agente, " +
                                                                                "apellido_agente ) " +
                                              "SELECT id_agente_llamada, " + 
                                                     "'unknown', " +
                                                     "0, " +
                                                     "'unknown', " +
                                                     "'unknown'" +
                                              " FROM " + self.stg_table + "_new_agentes_ids; "
                                        )


    def mapear_keys_dimensiones_fk(self):
        self.dm_connector.execute_query(query="UPDATE " + self.stg_table +
                                              " SET " + "key_cliente_llamada" + " = dim_clientes_key_lookup(id_cliente_llamada) ") 
              
        self.dm_connector.execute_query(query="UPDATE " + self.stg_table +
                                              " SET " + "key_agente_llamada" + " = dim_agentes_key_lookup(id_agente_llamada) ")


    def cargar_tabla_fact(self):
        self.dm_connector.execute_query(query="DELETE FROM " + self.table + " f " +
                                              "WHERE fecha_llamada between '" + str(self.start_date) + "' and '" + str(self.end_date) + "';" + 
                                        
                                        "INSERT INTO " + self.table + " ( fecha_llamada, " + 
                                                                                "key_cliente_llamada, " + 
                                                                                "campania_llamada, " + 
                                                                                "telefono_llamada, " +
                                                                                "key_agente_llamada, " +
                                                                                "tiempo_hablado_llamada, " +
                                                                                "tiempo_total_llamada ) " + 
                                        "SELECT  stg.fecha_llamada, " + 
                                                 "stg.key_cliente_llamada, " + 
                                                 "stg.campania_llamada, " + 
                                                 "stg.telefono_llamada, " +
                                                 "stg.key_agente_llamada, " +
                                                 "stg.tiempo_hablado_llamada, " + 
                                                 "stg.tiempo_total_llamada  " +
                                        "FROM " + self.stg_table + " stg"                                    
                                     )


    def calcular_medidas(self):

        self.dm_connector.execute_query(query="WITH logins_post_llamada AS ( " +
                                                    "select key_llamada, fecha_llamada, key_login, fecha_login " +
                                                    "from fact_llamadas llam left join dim_logins lg " +
                                                                            "on llam.key_cliente_llamada = lg.key_cliente_login and " +
                                                                                "llam.fecha_llamada <= lg.fecha_login " +
                                                    "where EXTRACT(EPOCH FROM llam.tiempo_hablado_llamada) >= 30 and " +
                                                            "EXTRACT(EPOCH FROM lg.fecha_login - llam.fecha_llamada ) < 60*60*48 " +
                                                ")," +
                                                "min_fecha_login_post_llamada AS ( " +
                                                    "select key_llamada, fecha_llamada, MIN(fecha_login) min_fecha_login " +
                                                    "from logins_post_llamada " +
                                                    "GROUP BY key_llamada, fecha_llamada " +
                                                ")," +
                                                "primer_login_post_llamada AS ( " +
                                                    "select logins_post_llamada.*, min_fecha_login_post_llamada.min_fecha_login " +
                                                    "FROM logins_post_llamada inner join min_fecha_login_post_llamada " +
                                                                            "on logins_post_llamada.key_llamada = min_fecha_login_post_llamada.key_llamada and " +
                                                                                "logins_post_llamada.fecha_login = min_fecha_login_post_llamada.min_fecha_login " +
                                                ")" +
                                                "update fact_llamadas " +
                                                "set key_login_post_llamada = primer_login_post_llamada.key_login " +
                                                "from  primer_login_post_llamada " +
                                                "where fact_llamadas.key_llamada = primer_login_post_llamada.key_llamada ")


