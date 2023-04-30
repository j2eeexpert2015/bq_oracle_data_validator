import cx_Oracle
import oracle_credentials

# Connect to the Oracle Database
oracle_connection = cx_Oracle.connect(oracle_credentials.username, oracle_credentials.password, oracle_credentials.dsn)

# Prepare and execute the SQL query for the Oracle table
oracle_cursor = oracle_connection.cursor()

oracle_count_query = f"SELECT COUNT(*) FROM {oracle_credentials.table_name}"
oracle_cursor.execute(oracle_count_query)

# Get the Oracle record count
oracle_record_count = oracle_cursor.fetchone()[0]
print(f"Oracle Table Record Count: {oracle_record_count}")


oracle_query = f"""
    SELECT COLUMN_NAME, DATA_TYPE
    FROM USER_TAB_COLUMNS
    WHERE TABLE_NAME = :table_name
"""
oracle_cursor.execute(oracle_query, table_name=oracle_credentials.table_name.upper())

# Fetch the results and create the Oracle column data type map
oracle_column_data_type_map = {row[0]: row[1] for row in oracle_cursor.fetchall()}


# Compare and print matching columns and data types
print("Matching columns and data types:")
for column_name, oracle_data_type in oracle_column_data_type_map.items():
    print(f"Column Name: {column_name}, Oracle Data Type: {oracle_data_type}")

# Close the Oracle cursor and connection
oracle_cursor.close()
oracle_connection.close()
