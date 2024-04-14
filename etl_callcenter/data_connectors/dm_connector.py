from sqlalchemy import create_engine
from sqlalchemy import text as sql_text
import sys
import pandas as pd


class DMConnector:

    def __init__(self):
        self.server = 'aws-0-sa-east-1.pooler.supabase.com:5432'
        self.database = 'postgres'
        self.username = 'postgres.rhhuzfmugrokjdwgicqi'
        self.password = '095_check_plate'
        
        try:           
            self.engine = create_engine("postgresql+psycopg2://"+ 
                                        self.username +":"+ 
                                        self.password +"@"+ 
                                        self.server+"/"+ 
                                        self.database)
        except:
            error_message = 'Error - ' + str(sys.exc_info()[1])
            print(error_message)


    def execute_query(self, query: str):
        try:
            with self.engine.connect() as conn:
                conn.execute(sql_text(query))
                conn.commit()
        except:
            error_message = 'Error - ' + str(sys.exc_info()[1])
            print(error_message)

    
    def append_data_to_table(self, table: str, data: pd.DataFrame):
        try:
            data.to_sql(name=table, 
                        con=self.engine, 
                        if_exists='append',
                        index=False)
        except:
            error_message = 'Error - ' + str(sys.exc_info()[1])
            print(error_message)


    def select_query_into_dataframe(self, query: str):
        try:
            return pd.read_sql(query,
                               self.engine)
        except:
            error_message = 'Error - ' + str(sys.exc_info()[1])
            print(error_message)