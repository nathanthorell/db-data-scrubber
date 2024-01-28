from dataclasses import dataclass
from typing import List
from faker import Faker


@dataclass
class TableData:
    schema_name: str
    table_name: str
    column_name: str
    fake_data_type: str
    column_data_type: str = None
    unique_count: int = None
    source_data: List[str] = None
    fake_data: List[str] = None
    data_mapping: dict = None

    def get_unique_count(self, conn) -> int:
        cur = conn.cursor()
        cur.execute(
            f"SELECT count(DISTINCT {self.column_name}) FROM [{self.table_name}]"
        )
        self.unique_count = cur.fetchone()[0]
        cur.close()

    def create_fake_data(self, count: int = 1) -> List[str]:
        fake = Faker()
        if hasattr(fake, self.fake_data_type):
            fake_data_generator = getattr(fake, self.fake_data_type)
            self.fake_data = [fake_data_generator() for _ in range(count)]

    def get_column_data_type(self, conn):
        "Finds the column_data_type of the table column"
        crsr = conn.cursor()

        data_type_query = f"""
            SELECT DATA_TYPE + CASE
                WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL
                THEN '(' + CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + ')'
                ELSE '' END AS DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{self.schema_name}'
                AND TABLE_NAME = '{self.table_name}'
                AND COLUMN_NAME = '{self.column_name}'
            """
        crsr.execute(data_type_query)
        data_type = crsr.fetchone()[0]

        crsr.close()
        self.column_data_type = data_type

    def get_source_data(self, conn):
        crsr = conn.cursor()

        source_data_query = f"""
            SELECT DISTINCT {self.column_name}
            FROM [{self.table_name}]
        """
        crsr.execute(source_data_query)
        self.source_data = crsr.fetchall()

        crsr.close()

    def map_source_to_fake(self):
        "Map source data to fake data"
        # Assuming self.source_data is a list of pyodbc.Row objects with one column
        source_values = [row[0] for row in self.source_data]
        mapping_dict = dict(zip(source_values, self.fake_data))
        self.data_mapping = mapping_dict

    def build_merge_sql(self) -> str:
        "Builds a merge SQL statement based on the MapFakes staging table"
        if not self.data_mapping:
            return None

        merge_sql = f"""
            MERGE INTO [{self.schema_name}].[{self.table_name}] AS target
            USING [{self.schema_name}].[MapFakes] AS source
            ON target.{self.column_name} = source.[Source_{self.column_name}]
            WHEN MATCHED THEN
                UPDATE SET target.{self.column_name} = source.[Fake_{self.column_name}];
        """

        return merge_sql
