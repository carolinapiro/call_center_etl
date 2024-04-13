import pandas as pd

from data_connectors.dm_connector import DMConnector
from tables.BaseTables import DimTable


class Logins(DimTable):
    
    def __init__(self):
        self.stg_table = "stg_dim_logins"
        self.table = "dim_logins"
        self.id = "id_login"
        self.dm_connector = DMConnector()

        self.source_data_type = 'json'
        self.source_data_path = 'source_files/logins_2024.json'



    def extraer_datos_fuente(self):
        self.source_data_df = pd.read_json(self.source_data_path)


    def limpiar_datos_fuente(self):
        self.source_data_df_clean = self.source_data_df

        # Change data types - for all fields
        self.source_data_df_clean['callid'] = self.source_data_df_clean['callid'].astype('int')
        self.source_data_df_clean['logindate'] = self.source_data_df_clean['logindate'].astype('datetime64[ns]')
        self.source_data_df_clean['customerid'] = self.source_data_df_clean['customerid'].astype('int')
        self.source_data_df_clean['channel'] = self.source_data_df_clean['channel'].astype('int')

        # Replace null values - for not pk or fk fields
        self.source_data_df_clean.fillna({'logindate':'01/01/1900'}, inplace=True)

        # Rename fields - for all fields
        self.source_data_df_clean.rename(columns = {'callid':'id_login'}, inplace = True) 
        self.source_data_df_clean.rename(columns = {'logindate':'fecha_login'}, inplace = True) 
        self.source_data_df_clean.rename(columns = {'customerid':'id_cliente_login'}, inplace = True) 
        self.source_data_df_clean.rename(columns = {'channel':'id_canal_digital_login'}, inplace = True) 
    

    def validar_dimensiones_fk(self):
        
        # Replace Null ids with default member's key
        self.dm_connector.execute_query(query="UPDATE " + self.stg_table +
                                              " SET " + "id_cliente_login" + " = -999 " + 
                                              " WHERE " + "id_cliente_login" + " is null ") 
              
        self.dm_connector.execute_query(query="UPDATE " + self.stg_table +
                                              " SET " + "id_canal_digital_login" + " = -999 " + 
                                              " WHERE " + "id_canal_digital_login" + " is null ")

        # Add inexistent ids to related dim table
        self.dm_connector.execute_query(query="DROP TABLE IF EXISTS " + self.stg_table + "_new_client_ids; " + 
                                              "CREATE TEMPORARY TABLE " + self.stg_table + "_new_client_ids " + " AS" +
                                              " SELECT DISTINCT " + "id_cliente_login" +
                                              " FROM " + self.stg_table +  
                                              " WHERE " + "id_cliente_login" + " NOT IN ("
                                                                                    "SELECT DISTINCT " + "id_cliente" +
                                                                                    " FROM " + "dim_clientes" +
                                                                                "); " +
                                              "INSERT INTO " + "dim_clientes" + "( id_cliente, " + 
                                                                                  "genero_cliente, " + 
                                                                                  "tipo_cliente ) " +
                                              "SELECT id_cliente_login, " + 
                                                     "'unknown', " +
                                                     "'unknown'" +
                                              " FROM " + self.stg_table + "_new_client_ids; "
                                        )

        self.dm_connector.execute_query(query="DROP TABLE IF EXISTS " + self.stg_table + "_new_canal_ids; " + 
                                              "CREATE TEMPORARY TABLE " + self.stg_table + "_new_canal_ids " + " AS" +
                                              " SELECT DISTINCT " + "id_canal_digital_login" +
                                              " FROM " + self.stg_table +  
                                              " WHERE " + "id_canal_digital_login" + " NOT IN ("
                                                                                    "SELECT DISTINCT " + "id_canal_digital" +
                                                                                    " FROM " + "dim_canales_digitales" +
                                                                                "); " + 
                                             "INSERT INTO " + "dim_canales_digitales" + "( id_canal_digital, " + 
                                                                                          "descripcion_canal_digital ) " +
                                              "SELECT id_canal_digital_login, " + 
                                                     "'unknown'" +
                                              " FROM " + self.stg_table + "_new_canal_ids; "
                                        )

    def mapear_keys_dimensiones_fk(self):
        self.dm_connector.execute_query(query="UPDATE " + self.stg_table +
                                              " SET " + "key_cliente_login" + " = dim_clientes_key_lookup(id_cliente_login) ") 
              
        self.dm_connector.execute_query(query="UPDATE " + self.stg_table +
                                              " SET " + "key_canal_digital_login" + " = dim_canales_digitales_key_lookup(id_canal_digital_login) ")
        

    def cargar_tabla_dim(self):
        self.dm_connector.execute_query(query="MERGE INTO " + self.table + " dim " +
                                        "USING " + self.stg_table + " stg " +
                                        "ON stg." + self.id + " = dim." + self.id +
                                        " WHEN MATCHED THEN " + 
                                            "UPDATE SET fecha_login = stg.fecha_login, " + 
                                                       "key_cliente_login = stg.key_cliente_login, " + 
                                                       "key_canal_digital_login = stg.key_canal_digital_login " + 
                                        " WHEN NOT MATCHED THEN " + 
                                            "INSERT ( id_login, " + 
                                                     "fecha_login, " + 
                                                     "key_cliente_login, " +
                                                     "key_canal_digital_login ) " + 
                                            "VALUES ( stg.id_login, " + 
                                                     "stg.fecha_login, " + 
                                                     "stg.key_cliente_login, " + 
                                                     "stg.key_canal_digital_login ) "                                       
                                     )