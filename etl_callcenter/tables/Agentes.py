import pandas as pd

from data_connectors.dm_connector import DMConnector
from tables.BaseTables import DimTable


class Agentes(DimTable):
    
    def __init__(self):
        self.stg_table = "stg_dim_agentes"
        self.table = "dim_agentes"
        self.id = "id_agente"

        self.source_data_type = 'csv'
        self.source_data_path = 'source_files/Agentes_2024.csv'

        self.dm_connector = None
        self.source_connector = None

        DimTable.__init__(self)


    def limpiar_datos_fuente(self):
        self.source_data_df_clean = self.source_data_df

        # Change data types - for all fields
        self.source_data_df_clean['idagente'] = self.source_data_df_clean['idagente'].astype('int')
        self.source_data_df_clean['agente'] = self.source_data_df_clean['agente'].astype('string')
        self.source_data_df_clean['nivelexpertise'] = self.source_data_df_clean['nivelexpertise'].astype('int')
        self.source_data_df_clean['nombre'] = self.source_data_df_clean['nombre'].astype('string')
        self.source_data_df_clean['apellido'] = self.source_data_df_clean['apellido'].astype('string')

        # Replace null values - for not pk or fk fields 
        self.source_data_df_clean.fillna({'agente':'unknown'}, inplace=True)
        self.source_data_df_clean.fillna({'nivelexpertise':0}, inplace=True)
        self.source_data_df_clean.fillna({'nombre':'unknown'}, inplace=True)
        self.source_data_df_clean.fillna({'apellido':'unknown'}, inplace=True)

        # Rename fields - for all fields
        self.source_data_df_clean.rename(columns = {'idagente':'id_agente'}, inplace = True) 
        self.source_data_df_clean.rename(columns = {'agente':'agente'}, inplace = True) 
        self.source_data_df_clean.rename(columns = {'nivelexpertise':'nivel_expertise_agente'}, inplace = True) 
        self.source_data_df_clean.rename(columns = {'nombre':'nombre_agente'}, inplace = True) 
        self.source_data_df_clean.rename(columns = {'apellido':'apellido_agente'}, inplace = True) 


    def cargar_tabla_dim(self):
        self.dm_connector.execute_query(query="MERGE INTO " + self.table + " dim " +
                                        "USING " + self.stg_table + " stg " +
                                        "ON stg." + self.id + " = dim." + self.id +
                                        " WHEN MATCHED THEN " + 
                                            "UPDATE SET agente = stg.agente, " + 
                                                       "nivel_expertise_agente = stg.nivel_expertise_agente, " + 
                                                       "nombre_agente = stg.nombre_agente, " + 
                                                       "apellido_agente = stg.apellido_agente " + 
                                        " WHEN NOT MATCHED THEN " + 
                                            "INSERT ( id_agente, " + 
                                                     "agente, " +
                                                     "nivel_expertise_agente, " + 
                                                     "nombre_agente, " + 
                                                     "apellido_agente ) " + 
                                            "VALUES ( stg.id_agente, " + 
                                                     "stg.agente, " + 
                                                     "stg.nivel_expertise_agente, " + 
                                                     "stg.nombre_agente, " + 
                                                     "stg.apellido_agente ) "                                       
                                     )