import sys
from pandas import DataFrame


class FileConnector:

    def __init__(self, source_path: str):
        self.source_path = source_path


class CsvConnector(FileConnector):
    pass


class JsonConnector(FileConnector):
    pass
