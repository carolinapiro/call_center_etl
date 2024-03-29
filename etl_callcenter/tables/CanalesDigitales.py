
import pandas as pd

from data_connectors.dm_connector import DMConnector
from tables.BaseTables import DimTable


class CanalesDigitales(DimTable):
    
    def __init__(self):
        self.stg_dim_table = "stg_dim_canales_digitales"
        self.dim_table = "dim_canales_digitales"
        self.dim_id = "id_canal_digital"
        self.dm_connector = DMConnector()

        self.source_data_path = 'source_files/CanalDigital_2024.csv'


    def truncar_tabla_stg(self):
        self.dm_connector.execute_query(query="TRUNCATE TABLE " + self.stg_dim_table )


    def extraer_datos_fuente(self):
        self.source_data_df = pd.read_csv(self.source_data_path, encoding='latin-1')


    def limpiar_datos_fuente(self):
        self.source_data_df_clean = self.source_data_df

        # Change data types  - for all fields
        self.source_data_df_clean['idcanal'] = self.source_data_df_clean['idcanal'].astype('int')
        self.source_data_df_clean['desc_canal'] = self.source_data_df_clean['desc_canal'].astype('string')

        # Replace null values - for not pk or fk fields 
        self.source_data_df_clean.fillna({'desc_canal':'unknown'}, inplace=True)

        # Rename fields - for all fields
        self.source_data_df_clean.rename(columns = {'idcanal':'id_canal_digital'}, inplace = True) 
        self.source_data_df_clean.rename(columns = {'desc_canal':'descripcion_canal_digital'}, inplace = True) 


    def cargar_tabla_stg(self):
        self.source_data_df_clean.to_sql(name=self.stg_dim_table, 
                                         con=self.dm_connector.engine, 
                                         if_exists='append',
                                         index=False)
    

    def cargar_tabla_dim(self):
        self.dm_connector.execute_query(query="MERGE INTO " + self.dim_table + " dim " +
                                        "USING " + self.stg_dim_table + " stg " +
                                        "ON stg." + self.dim_id + " = dim." + self.dim_id +
                                        " WHEN MATCHED THEN " + 
                                            "UPDATE SET descripcion_canal_digital = stg.descripcion_canal_digital " + 
                                        " WHEN NOT MATCHED THEN " + 
                                            "INSERT ( id_canal_digital, " + 
                                                     "descripcion_canal_digital ) " + 
                                            "VALUES ( stg.id_canal_digital, " + 
                                                     "stg.descripcion_canal_digital ) "                                       
                                     )
        
    def procesar_carga_canales(self):

        self.truncar_tabla_stg()
        self.extraer_datos_fuente()
        self.limpiar_datos_fuente()
        self.cargar_tabla_stg()
        self.cargar_tabla_dim()