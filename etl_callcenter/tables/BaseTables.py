from data_connectors.dm_connector import DMConnector
from data_connectors.source_connector import *


class Table:

    def __init__(self):
        self.dm_connector = DMConnector()

        if self.source_data_type == 'csv':
            self.source_connector = CsvConnector()
        elif self.source_data_type == 'json':
            self.source_connector = JsonConnector()


    def truncar_tabla_stg(self):
        self.dm_connector.execute_query(query="TRUNCATE TABLE " + self.stg_table )


    def extraer_datos_fuente(self):
        self.source_data_df = self.source_connector.extract_data_from_file(self.source_data_path)


    def limpiar_datos_fuente(self):
        pass


    def cargar_tabla_stg(self):
        self.dm_connector.append_data_to_table(table=self.stg_table, data=self.source_data_df_clean)


    def validar_dimensiones_fk(self):
        pass


    def mapear_keys_dimensiones_fk(self):
        pass
    

class DimTable(Table):
    
    def cargar_tabla_dim(self):
        pass


    def procesar_carga_dim(self):

        self.truncar_tabla_stg()
        self.extraer_datos_fuente()
        self.limpiar_datos_fuente()
        self.cargar_tabla_stg()
        self.cargar_tabla_dim()


    def procesar_carga_dim_fks(self):

        self.truncar_tabla_stg()
        self.extraer_datos_fuente()
        self.limpiar_datos_fuente()
        self.cargar_tabla_stg()
        self.validar_dimensiones_fk()
        self.mapear_keys_dimensiones_fk()
        self.cargar_tabla_dim()


class FactTable(Table):
    
    def extraer_datos_fuente(self):
        Table.extraer_datos_fuente(self)

        self.source_data_df[self.date_field] = pd.to_datetime(self.source_data_df[self.date_field])
        self.source_data_df = self.source_data_df[(self.source_data_df[self.date_field] >= self.start_date) &
                                                  (self.source_data_df[self.date_field] < self.end_date) ]


    def cargar_tabla_fact(self):
        pass
    

    def calcular_medidas(self):
        pass


    def procesar_carga_fact(self):

        self.truncar_tabla_stg()
        self.extraer_datos_fuente()
        self.limpiar_datos_fuente()
        self.cargar_tabla_stg()
        self.validar_dimensiones_fk()
        self.mapear_keys_dimensiones_fk()
        self.cargar_tabla_fact()
        self.calcular_medidas()