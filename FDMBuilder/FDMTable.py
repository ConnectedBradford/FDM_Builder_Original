# from google.cloud import bigquery
import datetime
from dateutil.parser import parse
from FDMBuilder.FDM_helpers import *
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import numpy as np
import pandas as pd
import warnings
warnings.filterwarnings("ignore", category=UserWarning)
warnings.filterwarnings("ignore", category=SyntaxWarning)

# Set global variables 
PROJECT = "yhcr-prd-phm-bia-core"
CLIENT = bigquery.Client(project=PROJECT)
DEMOGRAPHICS = f"{PROJECT}.CY_STAGING_DATABASE.src_DemoGraphics_MASTER"
MASTER_PERSON = f"{PROJECT}.CY_FDM_MASTER.person"

    
class FDMTable:
    """A Tool for preparing individual source tables for FDM build
    
    Primary function is to ensure table has 3 basic features:
    
    1. A person_id column
    2. An Event start date - parsed into a DATETIME
    3. An Event end date (if required) - also parsed
    
    Also includes a number of helper functions to facilitate the process
    of readying a table to build an FDM. 
    
    Args:
        source_table_id: string, id of source table in GCP. Can be in format
            project_id.dataset_id.table_id or dataset_id.table_id
        dataset_id: string, id of dataset in GCP where FDM is to be built
        
    Attributes:
        source_table_full_id: Full id of source table in GCP
        dataset_id = id of dataset where table is to be built in GCP
        table_id = id of table alone i.e. without dataset/project id
        full_table_id = id of table with project and datatset ids i.e. in
            project_id.dataset_id.table_id format
    """
    
    
    def __init__(self, source_table_id, dataset_id):
            
        # error checks to ensure table/dataset exist and that FDMTable
        # wont overwrite source dataset
        if not check_table_exists(source_table_id):
            raise ValueError(f"""
    {source_table_id} doesn't exist. Be sure to include the dataset id 
    (i.e. DATASET.TABLE) and double check spelling is correct.
            """)
        if not check_dataset_exists(dataset_id):
            raise ValueError(f"""
    Dataset {dataset_id} doesn't exist. Double check spelling and GCP then 
    try again.
            """)
        if dataset_id == source_table_id.split(".")[1]:
            raise ValueError("""
    The dataset_id specified contains the original source table. FDMTable builds 
    must be performed in a fresh dataset to maintain an unchanged copy of the 
    original source table. Create an empty dataset in which to build your FDM, 
    and re-initialise the FDMTable using this dataset.
            """)
        # add project_id to source_table_id if not already included - GCP
        # SQL engine will sometimes throw errors if project_id not specified
        # in queries
        if len(source_table_id.split(".")) == 2: 
            source_table_id = f"{PROJECT}." + source_table_id
        self.source_table_full_id = source_table_id
        if len(dataset_id.split(".")) == 2:
            dataset_id = dataset_id.split(".")[-1]
        self.dataset_id = dataset_id
        table_alias = source_table_id.split(".")[-1]
        self.table_id = table_alias
        full_table_id = f"{PROJECT}.{self.dataset_id}.{table_alias}"
        self.full_table_id = full_table_id
        self._build_not_completed_message = (
            "_" * 80 + "\n\n"  
            f"\t ##### BUILD PROCESS FOR {self.table_id} COULD NOT BE COMPLETED! #####\n"
            f"\tFollow the guidance provided above and then re-run .build() when you've\n"
            f"\tresolved the issues preventing the build from completing."
        )
        
    def _check_table_exists_in_dataset(func):
        """Decorator Function - ensures a copy of dataset exists
        
        Used when helper functions require a copy of the source data in the FDM
        dataset to work.
        """
        def return_fn(self, *args, **kwargs):
            if not check_table_exists(self.full_table_id):
                raise ValueError(f"""
    A copy of {self.full_table_id} doesn't yet exist in f"{self.dataset_id}.
    Try running .copy_table_to_dataset() and then try again """)
            else:
                return func(self, *args, **kwargs)
                
        return return_fn
    
    def _check_problems_table_doesnt_exist(func):
        """Decorator Function - ensures a problems table doesn't exist
        
        Problems tables contain entries from the source data
        that have been removed from the main table because of "problems" with 
        the respective entires e.g. entry might be dated before the birth
        date of the person, might be missing a person_id etc. Several helper
        functions perform manipulations, adding/removing columns etc. If these 
        the main table is manipulated without corresponding manipulation of the 
        problems table, it wouldn't then be possible to merge/union the tables 
        whilst FDM building. 
        
        This helper enforces the requirement that only a  complete table be 
        manipulated i.e. a table that has been "recombined" or  that doesn't 
        have an associated  problems table.
        """
        def return_fn(self, *args, **kwargs):
            if check_table_exists(self.full_table_id + "_fdm_problems"):
                raise ValueError(f"""
    A {self.table_id}_fdm_problems table exists in {self.dataset_id}. 
    {self.table_id} should be 'recombined' with problem entries 
    before any manipulations/changes to the table are performed. Run .recombine() 
    and then try again""")
            else:
                return func(self, *args, **kwargs)
                
        return return_fn
        
    def check_build(self, verbose=True):
        """Checks all necessary parts of Table build have been completed
        
        Used to ensure a table is ready before building the FDM datset. Checks
        that: 
            1. a copy of the table exists in the dataset
            2. the table has a person_id column
            3. the table has an fdm_start_date column
            4. the table has an fdm_end_date column
            5. if there's a corresponding problems table in the dataset

        Returns:
            A tuple of boolean values representing the 5 checks above
        """
        table_exists = check_table_exists(self.full_table_id)
        if table_exists:
            column_names = self.get_column_names()
            person_id_present = "person_id" in column_names
            fdm_start_present = "fdm_start_date" in column_names
            fdm_end_present = "fdm_end_date" in column_names
            fdm_end_present = "fdm_end_date" in column_names
            problem_table_present = check_table_exists(self.full_table_id 
                                                       + "_fdm_problems")
        else:
            person_id_present = False
            fdm_start_present = False
            fdm_end_present = False
            problem_table_present = False
        return (table_exists, person_id_present, fdm_start_present, 
                fdm_end_present, problem_table_present)
        
    
    def build(self):
        """Prepares table for FDM build with prompts and user input
        
        No arguments required, simply run `build` and follow the instructions
        in the console output, inputting the requested info as required. Designed
        so build process can be cancelled (by stopping execution) and re-running
        when ready - the script will pick up where it left off.
        
        Returns:
            None - all changes occurr in GCP
        """
        
        print(f"\t ##### BUILDING FDM TABLE COMPONENTS FOR {self.table_id} #####")
        print("_" * 80 + "\n")

        print(f"1. Copying {self.table_id} to {self.dataset_id}:")
        self._copy_table_to_dataset_w_inputs()

        print(f"\n2. Adding person_id column:")
        identifier_added = self._add_person_id_to_table_w_inputs()
        if not identifier_added:
            print(self._build_not_completed_message)
            return None

        print(f"\n3. Adding fdm_start_date column:")
        fdm_start_added = self._add_fdm_start_date_w_inputs()
        if not fdm_start_added:
            print(self._build_not_completed_message)
            return None
        print(f"\n4. Adding fdm_end_date column:")
        fdm_end_added = self._add_fdm_end_date_w_inputs()
        if not fdm_start_added:
            print(self._build_not_completed_message)
            return None

        print("_" * 80 + "\n")
        print(f"\t ##### BUILD PROCESS FOR {self.table_id} COMPLETE! #####\n")
    
    
    def quick_build(self, fdm_start_date_cols, fdm_start_date_format,
                    fdm_end_date_cols=None, fdm_end_date_format=None,
                    verbose=True):
        """Performs the table build process without verbose user input

        Adds the 3 basic FDM table features:  1. A person_id column  2. An Event 
        start date - parsed into a DATETIME 3. An Event end date 
        (if required) without console input as with `build`.

        Args:
            fdm_start_date_cols: string/list, name of individual column that 
                contains a parse-able string with day, month and year -- or -- 
                list with  columns containing day, month and year info - list 
                will also accept static values  e.g. "15", "Feb", "November", 
                "2022" in the event  one of D/M/Y isn't  available and a static 
                value will suffice e.g. setting day as 15th for each date.
            fdm_start_date_format: string, one of "YMD" (Default), "YDM",  
                "DMY", "MDY". Order in which day, month and year appear - required
                for both string and list fdm_start_date_cols
            fdm_end_date_cols: string/list, same format as fdm_start_date_cols
                but with col(s) containing end date info. Can be None/left blank 
                should there not be an end date (FDM process will then calculate 
                observation periods using start date only)
            fdm_end_date_format: string, one of "YMD" (Default), "YDM",  
                "DMY", "MDY". Same format as fdm_start_date_format, can be 
                None/left blank if fdm_end_date_cols is blank
            verbose: bool (default True), controls console output showing progress 
                of build
                
        Returns:
            None - all changes occurr in GCP
        """
        if verbose:
            print(f"Building {self.table_id}:")
        self.copy_table_to_dataset(verbose=verbose)
        self._add_person_id_to_table(verbose=verbose)
        start_dates_parsed = self._add_parsed_date_to_table(
            date_cols=fdm_start_date_cols,  
            date_format=fdm_start_date_format,  
            date_column_name="fdm_start_date"
        )
        if start_dates_parsed:
            print("    fdm_start_date column added")
        else:
            print("    fdm_start_date could not be parsed with inputs provided")
        if fdm_end_date_cols is not None:
            end_dates_parsed = self._add_parsed_date_to_table(
                date_cols=fdm_end_date_cols,  
                date_format=fdm_end_date_format,  
                date_column_name="fdm_end_date"
            )
            if end_dates_parsed:
                print("    fdm_end_date column added")
            else:
                print("    fdm_end_date could not be parsed with inputs provided")
        else:
            print("    no fdm_end_date info provided")
        print("Done.")
    
    
    @_check_table_exists_in_dataset
    def get_column_names(self):
        """Lists the table's column names
        
        Returns: 
            list, strings detailing each column name
        """
        
        table = CLIENT.get_table(self.full_table_id)
        return [field.name for field in table.schema]
            
            
    @_check_table_exists_in_dataset
    @_check_problems_table_doesnt_exist
    def add_column(self, column_sql):
        """Adds a column to the table according to user specification
        
        Args:
            colunm_sql: string, a sql statement that specifies the new
                column similar to that would see in a standard SELECT statement
                
        Returns:
            None - changes occurr in GCP
            
        Example:
        
        ```python
        # adds new column called integer_col_x_100 
        # containing integer_col * 100 
        my_table.add_column(
            "integer_col * 100 AS integer_col_x_100"
        )
        # adds new column called string_col_first_item
        # containing first item in string_col divided by "/"
        my_table.add_column(
            "SPLIT(string_col, "/")[OFFSET(0)] AS string_col_first_item"
        )
        ```
        """
        sql = f"""
            SELECT *, {column_sql}
            FROM `{self.full_table_id}`
        """
        run_sql_query(sql, destination=self.full_table_id)
    
    
    @_check_table_exists_in_dataset
    @_check_problems_table_doesnt_exist
    def drop_column(self, column):
        """Drops/deletes the specified column
        
        Args:
            colunm: string, name of column to be deleted
                
        Returns:
            None - changes occurr in GCP
        """
        sql = f"""
            ALTER TABLE `{self.full_table_id}`
            DROP COLUMN {column}
        """
        run_sql_query(sql)
    
    
    @_check_table_exists_in_dataset
    @_check_problems_table_doesnt_exist
    def rename_columns(self, names_map, verbose=True):
        """Renames columns of table
        
        Args:
            names_map: dict, key-value pairs are strings, keys detailing
            current names, values the new names columns should be renamed
            to
                
        Returns:
            None - changes occurr in GCP
            
        Example:
        ```python
        # renames columns from old_name_1/2 to new_name_1/2
        my_table.rename_columns(
            {"old_name_1":"new_name_1,
             "old_name_2":"new_name_2}
        )
        ```
        """
        rename_columns_in_bigquery(table_id=self.full_table_id,
                                   names_map=names_map,
                                   verbose=verbose)
        
        
    @_check_table_exists_in_dataset
    @_check_problems_table_doesnt_exist
    def head(self, n=10):
        """Displays first n rows of table as pandas dataframe
        
        Args:
            n: int, number of rows from table to return
            
        Returns:
            pandas.DataFrame, containing first n rows of data from
                table
        """
        sql = f"""
            SELECT *
            FROM `{self.full_table_id}`
            LIMIT {n}
        """
        return pd.read_gbq(sql)
    
    
    @_check_table_exists_in_dataset
    def _get_table_schema_dict(self):
        """SHORT DESCRIPTION OF FN

        LONDGER DESCRIPTION HERE

        Args:
                
        Returns:
        
        Example:
        ```python
        ```
        """
        table_schema = CLIENT.get_table(self.full_table_id).schema
        return {field.name: field.field_type  
                for field in table_schema}
                                                                                                          
    
    @_check_table_exists_in_dataset
    def build_data_dict(self):
        """Builds starting data dictionary and uploads to GCP

        LONDGER DESCRIPTION HERE

        Args:
                
        Returns:
        
        Example:
        ```python
        ```
        """
        
        schema_dict = self._get_table_schema_dict()
        data_dict = {
            "variable_name": [],
            "data_type": [],
            "description": [],
        }
        for col_name, col_dtype in schema_dict.items():
            data_dict["variable_name"].append(col_name)
            data_dict["data_type"].append(col_dtype)
            n_unique_values_sql = f"""
                SELECT COUNT(DISTINCT {col_name}) AS n, 
                FROM `{self.full_table_id}`
                WHERE {col_name} IS NOT NULL
            """
            n_unique_values_df = pd.read_gbq(n_unique_values_sql)
            n_unique_values = n_unique_values_df.n[0]
            
            if col_dtype in ["INTEGER", "DATETIME", "FLOAT"]:
                data_sql = f"SELECT MIN({col_name}) AS min_val, "
                data_sql += f"MAX({col_name}) AS max_val"
                if col_dtype != "DATETIME":
                    data_sql += (f", AVG({col_name}) "
                                 "AS mean_val")
                data_sql += f" FROM `{self.full_table_id}`"
                data_sql += f" WHERE {col_name} IS NOT NULL"
                data_df = pd.read_gbq(data_sql)
                
                description = f"{n_unique_values} Unique Values - "
                description = f"Min: {data_df.min_val[0]}, "
                description += f"Max: {data_df.max_val[0]}"
                if col_dtype != "DATETIME":
                    description += f", Mean: {data_df.mean_val[0]}, "
            elif n_unique_values > 20:
                unique_values_sql = f"""WITH src AS (
                    SELECT * FROM `{self.full_table_id}`
                    WHERE {col_name} IS NOT NULL
                    LIMIT 1000
                )
                SELECT ARRAY_AGG(DISTINCT {col_name}) AS unique_values 
                FROM src
                """
                unique_values_df = pd.read_gbq(unique_values_sql)
                values = unique_values_df.unique_values[0]
                description = f"{n_unique_values} unique Values - Examples: " 
                description += ", ".join(
                    [str(val) for val in values[:5]]
                )
            else:
                unique_values_sql = f"""
                    SELECT ARRAY_AGG(DISTINCT {col_name}) AS unique_values 
                    FROM `{self.full_table_id}`
                    WHERE {col_name} IS NOT NULL
                """
                unique_values_df = pd.read_gbq(unique_values_sql)
                values = unique_values_df.unique_values[0]
                description = f"{n_unique_values} unique Values: " 
                description += ", ".join(
                    [str(val) for val in values]
                )
            data_dict["description"].append(description)
        data_dict_df = pd.DataFrame(data_dict)
        data_dict_df.to_gbq(destination_table=self.full_table_id + "_data_dict", 
                            project_id=PROJECT, 
                            if_exists="replace", 
                            progress_bar=False)
    
    
    def copy_table_to_dataset(self, overwrite_existing=False, verbose=False):
        """SHORT DESCRIPTION OF FN

        LONDGER DESCRIPTION HERE

        Args:
                
        Returns:
        
        Example:
        ```python
        ```
        """
        
        src_copy_exists = check_table_exists(self.full_table_id)
        
        if src_copy_exists and not overwrite_existing:
            if verbose:
                print(f"    using existing copy of {self.table_id} in " 
                      f"{self.dataset_id}")
            return None
        else:
            sql = f"""
                SELECT * 
                FROM `{self.source_table_full_id}`
            """
            run_sql_query(sql, destination=self.full_table_id)
            if verbose:
                print(f"    {self.table_id} copied to {self.dataset_id}")
            return None
            
    
    def recombine(self):
        """SHORT DESCRIPTION OF FN

        LONDGER DESCRIPTION HERE

        Args:
                
        Returns:
        
        Example:
        ```python
        ```
        """
        if not check_table_exists(self.full_table_id + "_fdm_problems"):
            raise ValueError(f"{self.table_id} has no corresponding fdm "
                             "problems table in {self.dataset_id}")
        recombine_sql = f"""
            SELECT * 
            FROM {self.full_table_id + "_fdm_problems"}
            UNION ALL
            SELECT NULL AS fdm_problem, *
            FROM {self.full_table_id}
        """
        run_sql_query(recombine_sql, destination=self.full_table_id)
        CLIENT.delete_table(self.full_table_id + "_fdm_problems")
        
        
    def _add_person_id_to_table(self, verbose=False):
        """SHORT DESCRIPTION OF FN

        LONDGER DESCRIPTION HERE

        Args:
                
        Returns:
        
        Example:
        ```python
        ```
        """
        
        correct_identifiers = ["person_id", "digest", "EDRN"]
        identifiers_in_src = [col for col in self.get_column_names()
                              if col in correct_identifiers] 
        # find matching identifier columns and correct syntax if required
        if not identifiers_in_src:
            raise ValueError(
                f"None of person_id, digest, or EDRN in table columns"
            )
        
        if "person_id" in self.get_column_names():
            if verbose:
                print(f"    {self.table_id} already contains person_id column")
        else:
            if "digest" in self.get_column_names():
                identifier = "digest"
            else:
                identifier = "EDRN" 
            sql = f"""
                SELECT demo.person_id, src.*
                FROM `{self.full_table_id}` src
                LEFT JOIN `{DEMOGRAPHICS}` demo
                ON src.{identifier} = demo.{identifier}
            """
            run_sql_query(sql, destination=self.full_table_id)
            person_id_df = pd.read_gbq(f"SELECT person_id FROM {self.full_table_id}")
            if person_id_df.person_id.isna().all():
                raise ValueError(
                    "none of identifier column entries have corresponding " 
                    "person_id - join\nresulted in all NULL values"
                )
            if verbose:
                print("    person_id column added")
            
            
    def _get_fdm_date_df(self, date_cols, yearfirst, dayfirst):
        """SHORT DESCRIPTION OF FN

        LONDGER DESCRIPTION HERE

        Args:
                
        Returns:
        
        Example:
        ```python
        ```
        """

        schema_dict = self._get_table_schema_dict()
        if type(date_cols) == list and len(date_cols) == 3:
            cast_cols_sql = []
            for col in date_cols:
                if col in schema_dict.keys() and schema_dict[col] == "STRING":
                    cast_cols_sql.append(col)
                elif col in schema_dict.keys(): 
                    cast_cols_sql.append(f"CAST({col} AS STRING)")
                else:
                    cast_cols_sql.append(f'"{col}"')
            to_concat_sql = ', "-", '.join(cast_cols_sql) 
            sql = f"""
                SELECT uuid, CONCAT({to_concat_sql}) AS date
                FROM `{self.full_table_id}`
            """
        else:
            sql = f"""
                SELECT uuid, {date_cols} AS date
                FROM `{self.full_table_id}`
            """

        dates_df = pd.read_gbq(query=sql, project_id=PROJECT)
        
        def date_is_short(date):
            if type(date) is str and len(date) <= 8:
                return True
            elif not date:
                return True
            else:
                return False
        if all(dates_df.date.apply(date_is_short)):
                print("""
    WARNING: 2 character years are ambiguous e.g. 75 will be parsed as 1975 but 
    70 will be parsed as 2070. Consider converting year.
                """)
        def parse_date(x):
            if type(x) is datetime.datetime:
                x = x.date
            try:
                return parse(str(x), dayfirst=dayfirst, yearfirst=yearfirst)
            except:
                return None
        dates_df["parsed_date"] = dates_df.date.apply(parse_date)
        return dates_df[["uuid", "parsed_date"]]


    def _add_parsed_date_to_table(self, date_cols, date_format, date_column_name):
        """SHORT DESCRIPTION OF FN

        LONDGER DESCRIPTION HERE

        Args:
                
        Returns:
        
        Example:
        ```python
        ```
        """
        
        input_is_len_3_list = type(date_cols) == list and len(date_cols) == 3
        input_is_string = type(date_cols) == str
        if not input_is_len_3_list and not input_is_string:
            raise ValueError("Date cols must be either:\n    1. list naming "
                             "cols or static values containing day/month/year "
                             "info\n    2. string naming one column containing "
                             "date info")
            
        date_format_settings = {
            "YMD": [True, False],
            "YDM": [True, True],
            "DMY": [False, True],
            "MDY": [False, False]
        }
        
        schema_dict = self._get_table_schema_dict()
        if type(date_cols) == str and schema_dict[date_cols] in ["DATE", "DATETIME"]:
            self.add_column(f"{date_cols} as {date_column_name}")
            return True

        if "uuid" not in self.get_column_names():
            add_uuid_sql = f"""
                SELECT GENERATE_UUID() AS uuid, *
                FROM `{self.full_table_id}`
            """
            run_sql_query(add_uuid_sql, destination=self.full_table_id)

        yearfirst, dayfirst = date_format_settings[date_format]
        dates_df = self._get_fdm_date_df(date_cols, 
                                           yearfirst=yearfirst,
                                           dayfirst=dayfirst)
        
        if dates_df.parsed_date.isna().all():
            self.drop_column("uuid")
            return False
        
        temp_dates_id = f"{PROJECT}.{self.dataset_id}.tmp_dates"
        dates_df.to_gbq(destination_table=temp_dates_id,
                        project_id=PROJECT,
                        table_schema=[{"name":"parsed_date", "type":"DATETIME"}],
                        if_exists="replace",
                        progress_bar=False)

        join_dates_sql = f"""
            SELECT dates.parsed_date AS {date_column_name}, src.*
            FROM `{self.full_table_id}` AS src
            LEFT JOIN `{temp_dates_id}` as dates
            ON src.uuid = dates.uuid
        """
        run_sql_query(join_dates_sql, destination=self.full_table_id)

        self.drop_column("uuid")

        CLIENT.delete_table(temp_dates_id)
        
        return True
    
    
    def _copy_table_to_dataset_w_inputs(self): 
        """SHORT DESCRIPTION OF FN

        LONDGER DESCRIPTION HERE

        Args:
                
        Returns:
        
        Example:
        ```python
        ```
        """
        
        overwrite_existing = False
        if check_table_exists(self.full_table_id):
            response = input(f"""
        A copy of {self.table_id} already exists in {self.dataset_id}. 
        You can continue with the existing {self.table_id} table in {self.dataset_id}
        or make a fresh copy from the source dataset."   

        Continue with existing copy?
        > Type y or n: """)
            while response not in ["y", "n"]:
                response = input("\n\tYour response didn't match y or n."
                                 "\n\t> Try again: ")
            overwrite_existing = response == "n"
        
        self.copy_table_to_dataset(overwrite_existing=overwrite_existing,
                                   verbose=True)
            
            
    def _add_person_id_to_table_w_inputs(self):
        """SHORT DESCRIPTION OF FN

        LONDGER DESCRIPTION HERE

        Args:
                
        Returns:
        
        Example:
        ```python
        ```
        """
        
        correct_identifiers = ["person_id", "digest", "EDRN"]
        identifiers_in_src = [col for col in self.get_column_names()
                              if col in correct_identifiers] 
        if identifiers_in_src:
            print(f"    {self.table_id} already contains person_id column")
            return True
        
        col_names_list_string = "".join(
            ["\n\t\t" + name for name in self.get_column_names()]
        )
        response = input(f"""
    No identifier columns found! FDM process requires a person_id column 
    in each table -  or  a digest/EDRN column to be able to link  person_ids.
    person_id/digest/EDRN columns may be present under a different name - do any 
    of the following colums contain digests or EDRNs? 
    (Note: identifiers are case sensitive)
    {col_names_list_string}
    
    If so, type the column in question. If not, type n.
    > Response: """)
        while not response in self.get_column_names() + ["n"]:
            response = input(f"""
    Response needs to match one of the above column names (case sensitive) or n
    > Response: """)
        if response == "n":
            print("""
    No identifier column to join person_id - have a discussion  with CYP data 
    team to establish a way forward.
            """)
            return False
        miss_named_id_col = response 
        response = input(f"""
    Does {miss_named_id_col} contain person_ids, digests or EDRNs?
    > Type either person_id, digest or EDRN: """)
        while response not in ["person_id", "digest", "EDRN"]:
            response = input(f"""
    Response needs to match one of person_id, digest or EDRN and is 
    case-sensitive.
    > Response: """)
        print("\n")
        self.rename_columns({miss_named_id_col: response})
        try: 
            self._add_person_id_to_table(verbose=True)
            return True
        except:
            print("""
    identifier column doesn't match with any person_ids and so all join results
    are NULL - check the identifier column details again and then re-run build
    
    NOTE: You'll need to start with a fresh copy of the source data 
    otherwise the same join will be attempted
            """)
            return False
        
        
    def _add_fdm_start_date_w_inputs(self):
        """SHORT DESCRIPTION OF FN

        LONDGER DESCRIPTION HERE

        Args:
                
        Returns:
        
        Example:
        ```python
        ```
        """
        
        if "fdm_start_date" in self.get_column_names():
            response = input(f"""
    fdm_start_date column is already present.
    
    You can continue with the existing fdm_start_date column or rebuild
    a new fdm_start_date_column from scratch.
    
    Continue with existing fdm_start_date? 
    > Type y or n: """)
            while response not in ["y", "n"]:
                response = input("    Your response didn't match y or n.\n"
                                 "    > Try again: ")
            if response == "y":
                return True
            else:
                self.drop_column("fdm_start_date")
        
        single_col_y_n = input(f"""
    An event start date is required to build the observation_period table. This 
    information should be contained within one or more columns of your table. 
    If unsure a quick look at the table data in BigQuery should clarify.
    
    Is the event start date found in one column that can be easily 
    parsed with a day, month and year? (The parser is pretty good at understanding
    most formats, including month names rather than numbers)
    > Type y or n """)
        while single_col_y_n not in ["y", "n"]:
            single_col_y_n = input("    Your response didn't match y or n.\n"
                                       "    > Try again: ")
        if single_col_y_n == "y":
            fdm_start_date_cols = input("""
    Which column contains the event start date?
    > Type the name (case sensitive): """)
            while fdm_start_date_cols not in self.get_column_names():
                fdm_start_date_cols = input(f"""
    {fdm_start_date_cols} doesn't match any of the columns in {self.table_id}"
    > Try again: """)
            fdm_start_date_format = input("""
    What format does the date appear in YMD/YDM/DMY/MDY?
    > Type one: """)
            while fdm_start_date_format not in ["YMD", "YDM", "DMY", "MDY"]:
                fdm_start_date_format = input("""
    Response must be one of YMD/YDM/DMY/MDY."
    > Try again: """)
        else:
            year = input("""
    We'll build the event start date beginning with identifying the year.  
    Your response can be the name of a column that contains the year - it must be 
    the year only, other formats like Apr-2020 can't be parsed.  Otherwise static 
    values are accepted i.e. you can type `2022` and the year will be parsed as 
    2022 in every date.
    
    If the year information isn't contained in one column, type `quit` as your 
    response, add a column with the year information and then re-run .build(). 
    You may find the .add_column() method useful for this.
    
    Where can the year information be found?
    > Response: """)
            if year == "quit":
                return False
            month = input("""
    And now we'll move onto the month. The same guidance as above applies.
    Remebmer, a static value like 02, or `Feb`, or `February` is acceptable.
    Where can the event start month be found?
    > Response:  """)
            if month == "quit":
                return False
            day = input("""
    And then day. Again a static day like 15 is fine.
    
    Where can the event start day be found?
    > Response: """)
            if day == "quit":
                return False
            fdm_start_date_cols = [year, month, day]
            fdm_start_date_format = "YMD"
            print("\n    adding fdm_start_date_column...")
                
        dates_parsed = self._add_parsed_date_to_table(
            date_cols=fdm_start_date_cols,  
            date_format=fdm_start_date_format,  
            date_column_name="fdm_start_date"
        )
        if dates_parsed:
            print("    fdm_start_date column added")
            return True
        else:
            print("""
    Looks like something went wrong and the parser couldn't understand the date
    information you provided. Check your responses above and re-run .build() if
    you notice any errors. Otherwise, seek help from the CYP data team.""")
            return False
        
        
    def _add_fdm_end_date_w_inputs(self):
        """SHORT DESCRIPTION OF FN

        LONDGER DESCRIPTION HERE

        Args:
                
        Returns:
        
        Example:
        ```python
        ```
        """
        
        if "fdm_end_date" in self.get_column_names():
            response = input("""
    fdm_end_date column is already present.
    You can continue with the existing fdm_end_date column or rebuild a new 
    fdm_end_date_column from scratch.
    Continue with existing fdm_end_date?
    > Type y or n: """)
            while response not in ["y", "n"]:
                response = input("\n    Your response didn't match y or n."
                                 "\n    > Try again: ")
            if response == "y":
                return True
            else:
                self.drop_column("fdm_end_date")
        
        has_fdm_end_date = input("""
    An event end date may or may not be relevant to this source data. For example, 
    hospital visits or academic school years have an end date as well as a start 
    date.
    
    If you're unsure weather or not the source data should include an event end 
    date, seek help from the CYP data team."
    
    Does this data have an event end date?"
    > Type y or n: """)
            
        while has_fdm_end_date not in ["y", "n"]:
            has_fdm_end_date = input("\n    Your response didn't match y or n."
                                   "\n    > Try again: ")

        if not has_fdm_end_date == "y":
            return True

        single_col_y_n = input("""
    The process will now proceed in exactly the same way as with the event start 
    date. Refer to the guidance above if at all unsure about the responses to any
    of the following questions.
    
    Is the event end date found in one column that can be easily parsed with a 
    day, month and year?
    > Type y or n: """)
        while single_col_y_n not in ["y", "n"]:
            single_col_y_n = input("\n    Your response didn't match y or n."
                                   "\n    > Try again: ")

        if single_col_y_n == "y":
            fdm_end_date_cols = input(
                "\n    Which column contains the event end date."
                "\n    > Type the name (case sensitive): "
            )
            while fdm_end_date_cols not in self.get_column_names():
                fdm_end_date_cols = input(f"""
    {fdm_end_date_cols} doesn't match any of the columns in {self.table_id}"
    > Try again: """)
            fdm_end_date_format = input("""
    What format does the date appear in? YMD/YDM/DMY/MDY
    > type one: """)
            while fdm_end_date_format not in ["YMD", "YDM", "DMY", "MDY"]:
                fdm_end_date_cols = input("""
    Response must be one of YMD/YDM/DMY/MDY.
    > Try again: """)
        else:
            year = input("""
    Where can the event end year be found?
    > Response: """)
            if year == "quit":
                return False
            month = input("""
    Where can the event end month be found?
    > Response: """)
            if year == "quit":
                return False
            day = input("""
    Where can the event end day be found?
    > Response: """)
            if year == "quit":
                return False
            fdm_end_date_cols = [year, month, day]
            fdm_end_date_format = "YMD"
                
        dates_parsed = self._add_parsed_date_to_table(
            date_cols=fdm_end_date_cols,  
            date_format=fdm_end_date_format,  
            date_column_name="fdm_end_date"
        )
        if dates_parsed:
            print("    fdm_end_date column added")
            return True
        else:
            print("""
    Looks like something went wrong and the parser couldn't understand the date
    information you provided. Check your responses above and re-run .build() if
    you notice any errors. Otherwise, seek help from the CYP data team. """)
            return False

        