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
    """Renames columns of a table in bigquery

    Args:
        table_id: string, full id (project_id.dataset_id.table_id) of the table 
            containing the columns to be renamed
        names_map: dict, key-value pairs are strings, keys detailing
            current names, values the new names of the columns
        verbose: bool, True to output progress information to console, False to
            suppress all console output

    Returns:
        None - changes occurr in GCP

    Example:
    ```python
    # renames columns from old_name_1/2 to new_name_1/2
    rename_columns_in_bigquery(
        table_id = "project_id.dataset_id.table_id",
        names_map = {"old_name_1":"new_name_1, 
                     "old_name_2":"new_name_2}
    )
    ```
    """
    
    alias_list = []
    if verbose:
        print("\tRenaming Columns:")
    for old_name, new_name in names_map.items():
        alias_list.append(f"{old_name} AS {new_name}")
        if verbose:
            print(f"\t{old_name} -> {new_name}")
    alias_string = ", ".join(alias_list)
    old_names_string = ", ".join(names_map.keys())
    n_table_cols = len(get_table_schema_dict(table_id))
    
    if len(names_map) == n_table_cols:
        sql = f"""
            SELECT {alias_string}
            FROM `{table_id}`
        """
    else:
        sql = f"""
            SELECT {alias_string}, * EXCEPT({old_names_string})
            FROM `{table_id}`
        """

    run_sql_query(sql=sql, destination=table_id)
    if verbose:
        print("\tRenaming Complete\n")
    
    
def clear_dataset(dataset_id):
    """Removes all tables from a dataset, leaving it empty

    Args:
        datset_id: string, full dataset id (i.e. "project_id.dataset_id")
            naming dataset to be cleared

    Returns:
        None - changes occurr in GCP
    """
    for table in CLIENT.list_tables(dataset_id):
        full_table_id = f"{dataset_id}.{table.table_id}"
        CLIENT.delete_table(full_table_id, not_found_ok=True)
        
        
def run_sql_query(sql, destination=None):
    """Quick way to run sql queries with bigquery library
    
    Can be used to run sql queries exactly as they would run using the 
    BigQuery SQL Workspace. By setting the "destination" argument, the results
    of a query can be stored as a new table/overwrite an existing table at the
    table id specified.

    Args:
        sql: string, the SQL command to be run
        destination: string (default: None), a table id where the results
            of the SQL command will be stored, if None then results aren't 
            stored

    Returns:
        bigquery.table.Table, containing table object of the stored results of 
            the query if destination argument isn't none
        -- otherwise --
        bigquery.job.queryjob, if no destination is provided and results aren't
            stored

    Example:
    ```python
    # queries example table and stores results in new table
    table = run_sql_query(
        sql = "SELECT * FROM `example.table.id`",
        destination = "destination.for.results"
    )
    
    # caps "value" column at 100
    query = run_sql_query(
        sql = "UPDATE example.table.id SET value = 100 WHERE value > 100"
    )
    ```
    """
    
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
    """Checks a dataset exists (surprisingly)
    
    Args:
        datset_id: full id of a dataset i.e. "project_id.datset_id"
        
    Returns:
        bool, True if dataset named in "dataset_id" exists, otherwise False
    """
    try:
        CLIENT.get_dataset(dataset_id)
        return True
    except:
        return False


def check_table_exists(full_table_id):
    """Checks a table exists (surprisingly)
    
    Args:
        table_id: full id of a table i.e. "project_id.datset_id.table_id"
        
    Returns:
        bool, True if table named in "table_id" exists, otherwise False
    """
    try:
        CLIENT.get_table(full_table_id)
        return True
    except:
        return False
        
        
def get_table_schema_dict(full_table_id):
    """Creates dictionary containing column name: column type 

    Takes the Schema object from the bigquery library and extracts
    the `name` and `field_type` attributes from each Field and stores
    them as key/value pairs in a python dictionary. The result is a 
    dictionary with keys for each column name in the source data, and 
    corresponding values for the data type of each column

    e.g:

    {
        "string_column_name": "STRING",
         "int_column_name": "INTEGER",
         ...
    }
    
    Args:
        full_table_id: string, table_id of table in bigquery for required
            schema. Must include project and dataset ids.

    Returns:
        dict, column name: colum data type pairs 
    """
    table = CLIENT.get_table(full_table_id)
    return {field.name: field.field_type  
            for field in table.schema}
                                                                                                          
    