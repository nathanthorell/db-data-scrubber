from TableData import TableData


def create_temp_table(conn, table: TableData):
    crsr = conn.cursor()

    source_column = f"Source_{table.column_name}"
    fake_column = f"Fake_{table.column_name}"

    # Clean up the staging table if needed
    cleanup_stage_table_sql = """
        DROP TABLE IF EXISTS [MapFakes];
    """
    crsr.execute(cleanup_stage_table_sql)

    # Create a stage table if it doesn't already exist
    create_staging_table_sql = f"""
        CREATE TABLE [{table.schema_name}].[MapFakes] (
            [{source_column}] {table.column_data_type} NOT NULL,
            [{fake_column}] {table.column_data_type} NOT NULL,
        )
    """
    crsr.execute(create_staging_table_sql)
    crsr.close()


def insert_data_into_temp_table(conn, table: TableData):
    crsr = conn.cursor()
    source_column = f"Source_{table.column_name}"
    fake_column = f"Fake_{table.column_name}"

    # Insert fake data into the staging table
    insert_fake_data_sql = f"""
        INSERT INTO [{table.schema_name}].[MapFakes]
        ([{source_column}], [{fake_column}])
        VALUES (?, ?)
    """
    crsr.executemany(
        insert_fake_data_sql,
        [
            (source_value, fake_value)
            for source_value, fake_value in table.data_mapping.items()
        ],
    )
    conn.commit()
    crsr.close()
