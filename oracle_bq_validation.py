import cx_Oracle
from google.cloud import bigquery
from google.oauth2 import service_account
import oracle_credentials
import bigquery_credentials

# Connect to the Oracle Database
oracle_connection = cx_Oracle.connect(oracle_credentials.username, oracle_credentials.password, oracle_credentials.dsn)

# Prepare and execute the SQL query for the Oracle table
oracle_cursor = oracle_connection.cursor()
oracle_query = f"""
    SELECT COLUMN_NAME, DATA_TYPE
    FROM USER_TAB_COLUMNS
    WHERE TABLE_NAME = :table_name
"""
oracle_cursor.execute(oracle_query, table_name=oracle_credentials.table_name.upper())

# Fetch the results and create the Oracle column data type map
oracle_column_data_type_map = {row[0]: row[1] for row in oracle_cursor.fetchall()}

credentials = service_account.Credentials.from_service_account_file(bigquery_credentials.bg_service_account_file_location)
# Initialize the BigQuery client
bigquery_client = bigquery.Client(credentials= credentials,project=bigquery_credentials.project_id)

# Get the BigQuery table schema
bigquery_table_ref = bigquery_client.dataset(bigquery_credentials.dataset_id).table(bigquery_credentials.table_id)
bigquery_table = bigquery_client.get_table(bigquery_table_ref)
#bq_table_record_count =bigquery_table.num_rows
# Create the BigQuery column data type map
bigquery_column_data_type_map = {field.name: field.field_type for field in bigquery_table.schema}

# Compare and print matching columns and data types
print("Matching columns and data types:")
for column_name, oracle_data_type in oracle_column_data_type_map.items():
    if column_name in bigquery_column_data_type_map:
        bigquery_data_type = bigquery_column_data_type_map[column_name]
        print(f"Column Name: {column_name}, Oracle Data Type: {oracle_data_type}, BigQuery Data Type: {bigquery_data_type}")

# Close the Oracle cursor and connection

# Get matching columns
matching_columns = set(oracle_column_data_type_map.keys()).intersection(bigquery_column_data_type_map.keys())

# Check for null values in the matching columns
for column in matching_columns:
    # Prepare and execute the SQL query for the Oracle table
    oracle_cursor = oracle_connection.cursor()
    oracle_null_count_query = f"SELECT COUNT(*) FROM {oracle_credentials.table_name} WHERE {column} IS NULL"
    oracle_cursor.execute(oracle_null_count_query)

    # Get the Oracle null count
    oracle_null_count = oracle_cursor.fetchone()[0]

    # Prepare and execute the SQL query for the BigQuery table
    bigquery_null_count_query = f"SELECT COUNT(*) FROM `{bigquery_credentials.dataset_id}.{bigquery_credentials.table_id}` WHERE {column} IS NULL"
    bigquery_query_job = bigquery_client.query(bigquery_null_count_query)

    # Get the BigQuery null count
    bigquery_null_count = list(bigquery_query_job.result())[0][0]

    if oracle_null_count > 0 or bigquery_null_count > 0:
        print(f"Column '{column}' has null values:")
        print(f"  - Oracle Table: {oracle_null_count} null values")
        print(f"  - BigQuery Table: {bigquery_null_count} null values")

# Close the Oracle cursor and connection
oracle_cursor.close()
oracle_connection.close()