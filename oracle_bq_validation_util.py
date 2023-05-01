oracle_bq_data_type_mapping = {'VARCHAR2':'STRING','NVARCHAR2':'STRING','CHAR':'STRING','NCHAR':'STRING','CLOB':'STRING','NCLOB':'STRING','INTEGER':'INT64','SHORTINTEGER':'INT64','LONGINTEGER':'INT64','NUMBER':'NUMERIC','DATE':'DATETIME','TIMESTAMP':'TIMESTAMP'}
#print(oracle_bq_data_type_mapping)
#for key,value in oracle_bq_data_type_mapping.items():
    #print("key:",key,",value:",value)

def validate_data_type_match(oracle_data_type, bq_data_type):
    oracle_data_type = str(oracle_data_type).strip().upper();
    bq_data_type = str(bq_data_type).strip().upper();
    match_result = "Unknown"
    if oracle_data_type in oracle_bq_data_type_mapping.keys():
        if oracle_bq_data_type_mapping[oracle_data_type]==bq_data_type:
            match_result = "Match"
        else:
            match_result ="Mismatch"
    match_result = match_result +","+oracle_data_type+" vs "+bq_data_type
    return match_result

