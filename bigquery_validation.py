from google.cloud import bigquery
from google.oauth2 import service_account
import bigquery_credentials

credentials = service_account.Credentials.from_service_account_file(bigquery_credentials.bg_service_account_file_location)
# Initialize the BigQuery client
bigquery_client = bigquery.Client(credentials= credentials,project=bigquery_credentials.project_id)

# Get the BigQuery table schema
bigquery_table_ref = bigquery_client.dataset(bigquery_credentials.dataset_id).table(bigquery_credentials.table_id)
bigquery_table = bigquery_client.get_table(bigquery_table_ref)

# Create the BigQuery column data type map
bigquery_column_data_type_map = {field.name: field.field_type for field in bigquery_table.schema}

# Compare and print matching columns and data types
print("Matching columns and data types:")
for column_name, oracle_data_type in bigquery_column_data_type_map.items():
    print("BigQuery Data Type: ",column_name,"Data Type:",column_name)

# Prepare and execute the SQL query for the BigQuery table
bigquery_count_query = f"SELECT COUNT(*) FROM `{bigquery_credentials.dataset_id}.{bigquery_credentials.table_id}`"
bigquery_query_job = bigquery_client.query(bigquery_count_query)

# Get the BigQuery record count
bigquery_record_count = list(bigquery_query_job.result())[0][0]
print(f"BigQuery Table Record Count: {bigquery_record_count}")
