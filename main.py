import json
import os
import pyodbc
from dotenv import load_dotenv
import TableData as td
import utils

# Load config and env
load_dotenv()
script_dir = os.path.dirname(os.path.realpath(__file__))
config_path = os.path.join(script_dir, "config.json")
with open(config_path) as f:
    config = json.load(f)
    mappings = config["mappings"]

# Build db connection string from env variables
conn_str = (
    f"DRIVER={os.getenv('DB_DRIVER')};"
    f"SERVER={os.getenv('DB_SERVER')},{os.getenv('DB_PORT')};"
    f"DATABASE={os.getenv('DB_DATABASE')};"
    f"UID={os.getenv('DB_USER')};"
    f"PWD={os.getenv('DB_PASS')};"
    f"Encrypt={os.getenv('DB_ENCRYPT')};"
)

conn = pyodbc.connect(conn_str)

# Loop through config mappings
for map in mappings:
    for column in map["columns"]:
        current_table_data = td.TableData(
            schema_name=map["schema"],
            table_name=map["table"],
            column_name=column["column_name"],
            fake_data_type=column["fake_data_type"],
        )

        current_name = (
            f"[{current_table_data.schema_name}].[{current_table_data.table_name}]"
        )
        print(f"Starting to process {current_name}")

        # get unique count and db column data type
        current_table_data.get_unique_count(conn)
        current_table_data.get_column_data_type(conn)

        # source and fake data
        if current_table_data.unique_count is not None:
            fake = current_table_data.create_fake_data(current_table_data.unique_count)

        current_table_data.get_source_data(conn)

        # now that we have the fake data, create the staging table
        utils.create_temp_table(conn, current_table_data)

        # map the fake data to the staging table
        current_table_data.map_source_to_fake()

        utils.insert_data_into_temp_table(conn, current_table_data)

        # Build and execute the merge SQL statement
        merge_sql = current_table_data.build_merge_sql()
        if merge_sql:
            conn.cursor().execute(merge_sql)
            conn.commit()

    print("")

conn.close()
