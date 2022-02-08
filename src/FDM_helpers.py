# from google.cloud import bigquery
from google.cloud import bigquery
import numpy as np
import pandas as pd
import pandas_gbq
import warnings
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=SyntaxWarning)

# Set global variables
PROJECT = "yhcr-prd-phm-bia-core"
CLIENT = bigquery.Client(project=PROJECT)
DEMOGRAPHICS = f"{PROJECT}.CY_MYSPACE_SR.demographics"


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
    
    job_config = bigquery.QueryJobConfig(
        destination=table_id,  
        write_disposition="WRITE_TRUNCATE"
    )
    CLIENT.query(sql, job_config=job_config).result()
    if verbose:
        print("Renaming Complete\n")
    
    
def clear_dataset(dataset_id):
    for table in CLIENT.list_tables(dataset_id):
        full_table_id = f"{PROJECT}.{dataset_id}.{table.table_id}"
        CLIENT.delete_table(full_table_id, not_found_ok=True)