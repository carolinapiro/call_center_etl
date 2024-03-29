
from sqlalchemy import create_engine
from sqlalchemy import text as sql_text
import sys

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
    