import pandas as pd

from data_connectors.dm_connector import DMConnector
from tables.BaseTables import DimTable


class Clientes(DimTable):
    
    def __init__(self):
        self.stg_table = "stg_dim_clientes"
        self.table = "dim_clientes"
        self.id = "id_cliente"

        self.source_data_type = 'csv'
        self.source_data_path = 'source_files/Customers_2024.csv'

        self.dm_connector = None
        self.source_connector = None

        DimTable.__init__(self)


    def limpiar_datos_fuente(self):
        self.source_data_df_clean = self.source_data_df

        # Change data types - for all fields
        self.source_data_df_clean['CustomerID'] = self.source_data_df_clean['CustomerID'].astype('int')
        self.source_data_df_clean['Gender'] = self.source_data_df_clean['Gender'].astype('string')
        self.source_data_df_clean['Type'] = self.source_data_df_clean['Type'].astype('string')

        # Replace null values - for not pk or fk fields
        self.source_data_df_clean.fillna({'Gender':'unknown'}, inplace=True)
        self.source_data_df_clean.fillna({'Type': 'unknown'}, inplace=True)

        # Rename fields - for all fields
        self.source_data_df_clean.rename(columns = {'CustomerID':'id_cliente'}, inplace = True) 
        self.source_data_df_clean.rename(columns = {'Gender':'genero_cliente'}, inplace = True) 
        self.source_data_df_clean.rename(columns = {'Type':'tipo_cliente'}, inplace = True) 
    

    def cargar_tabla_dim(self):
        self.dm_connector.execute_query(query="MERGE INTO " + self.table + " dim " +
                                        "USING " + self.stg_table + " stg " +
                                        "ON stg." + self.id + " = dim." + self.id +
                                        " WHEN MATCHED THEN " + 
                                            "UPDATE SET genero_cliente = stg.genero_cliente, " + 
                                                       "tipo_cliente = stg.tipo_cliente " + 
                                        " WHEN NOT MATCHED THEN " + 
                                            "INSERT ( id_cliente, " + 
                                                     "genero_cliente, " + 
                                                     "tipo_cliente ) " + 
                                            "VALUES ( stg.id_cliente, " + 
                                                     "stg.genero_cliente, " + 
                                                     "stg.tipo_cliente ) "                                       
                                     )
