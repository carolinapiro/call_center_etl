import sys
import pandas as pd


class FileConnector:

    def extract_data_from_file(self, path: str) -> pd.DataFrame:
        pass


class CsvConnector(FileConnector):
   
    def extract_data_from_file(self, path: str) -> pd.DataFrame:
        try:
            return pd.read_csv(path, encoding='latin-1')

        except:
            error_message = 'Error - ' + str(sys.exc_info()[1])
            print(error_message)


class JsonConnector(FileConnector):
    
    def extract_data_from_file(self, path: str) -> pd.DataFrame:
        try:
            return pd.read_json(path)

        except:
            error_message = 'Error - ' + str(sys.exc_info()[1])
            print(error_message)
