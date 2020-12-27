import csv

from typing import Sequence


class CSVOutputHandler:

    def __init__(self, fileName: str, tableFields: Sequence[str]):
        self.fileName = fileName
        self.tableFields = tableFields
        open(self.fileName, 'wt').write(f"{','.join(tableFields)}\n")
        self.file = open(self.fileName, 'at')
        self.csv = csv.DictWriter(self.file, fieldnames=tableFields, escapechar='\\', quotechar='"', quoting=csv.QUOTE_ALL)

    def add(self, data):
        self.csv.writerow(data)

    def close(self):
        self.file.close()
