# from google.cloud import bigquery
from google.cloud import bigquery
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=SyntaxWarning)

# Set global variables
PROJECT = "yhcr-prd-phm-bia-core"
CLIENT = bigquery.Client(project=PROJECT)


def rename_columns_in_bigquery(table_id, names_map, verbose=True):
    
    alias_list = []
    if verbose:
        print("Renaming Columns:")
    for old_name, new_name in names_map.items():
        alias_list.append(f"{old_name} AS {new_name}")
        if verbose:
            print(f"{old_name} -> {new_name}")
    alias_string = ", ".join(alias_list)
    old_names_string = ", ".join(names_map.keys())
    
    sql = f"""
        SELECT {alias_string}, * EXCEPT({old_names_string})
        FROM `{table_id}`
    """
    
    run_sql_query(sql=sql, destination=table_id)
    if verbose:
        print("Renaming Complete\n")
    
    
def clear_dataset(dataset_id):
    for table in CLIENT.list_tables(dataset_id):
        full_table_id = f"{dataset_id}.{table.table_id}"
        CLIENT.delete_table(full_table_id, not_found_ok=True)
        
        
def run_sql_query(sql, destination=None):
    
    if destination:
        job_config = bigquery.QueryJobConfig(
            destination=destination, 
            write_disposition="WRITE_TRUNCATE"
        )
    else:
        job_config=None
    
    query_job = CLIENT.query(sql, job_config=job_config)  # Make an API request.
    query_job.result()  # Wait for the job to complete.
    
    if destination:
        result_table = CLIENT.get_table(destination)
        return result_table
    else:
        return query_job

        
def check_dataset_exists(dataset_id):
    try:
        CLIENT.get_dataset(dataset_id)
        return True
    except:
        return False


def check_table_exists(full_table_id):
    try:
        CLIENT.get_table(full_table_id)
        return True
    except:
        return False
        
    