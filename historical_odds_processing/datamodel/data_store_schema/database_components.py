class Table:

    def __init__(self):
        self.tableName = None
        self.columns = None

    def create_table_sql(self):
        return f"""
            CREATE TABLE IF NOT EXISTS {self.tableName} 
            (
                {', '.join([str(col) for col in self.columns])},
            PRIMARY KEY ({','.join([col.name for col in self.columns if col.primaryKey])})
            );
        """

    def foreign_key_constraints(self):
        return []

    def get_indices(self):
        return [
            {
                'table': self.tableName,
                'column': column.name
            }
            for column in self.columns if column.isIndex
        ]

    def get_column_names(self):
        return [col.name for col in self.columns]


class Column:

    def __init__(self, name: str, dataType: str, primaryKey: bool = False, isIndex: bool = False):
        self.name = name
        self.dataType = dataType
        self.primaryKey = primaryKey
        self.isIndex = isIndex

    def __repr__(self):
        return f'{self.name} {self.dataType}'

    def __str__(self):
        return self.__repr__()


class ForeignKey:

    def __init__(self, columnName: str, tableReferenced: str, referenceColumn: str):
        self.columnName = columnName
        self.tableReferenced = tableReferenced
        self.referenceColumn = referenceColumn

    @property
    def constraintName(self):
        return f'{self.columnName}'

    def __repr__(self):
        return f'FOREIGN KEY ({self.columnName}) REFERENCES {self.tableReferenced} ({self.referenceColumn})'

    def __str__(self):
        return self.__repr__()

    def get_foreign_key_sql(self, tableName):
        return f"""
            ALTER TABLE {tableName} 
            ADD CONSTRAINT {self.columnName}
            FOREIGN KEY ({self.columnName}) REFERENCES {self.tableReferenced} ({self.referenceColumn});
        """
