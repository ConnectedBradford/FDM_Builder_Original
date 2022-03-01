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
    
    
    def __init__(self, source_table_full_id, dataset_id):
        
        self.source_table_full_id = source_table_full_id
        self.dataset_id = dataset_id
        table_alias = source_table_full_id.split(".")[-1]
        self.table_id = table_alias
        full_table_id = f"{PROJECT}.{self.dataset_id}.{table_alias}"
        self.full_table_id = full_table_id
        self._build_not_completed_message = (
            "_" * 80 + "\n\n"  
            f"\t ##### BUILD PROCESS FOR {self.table_id} COULD NOT BE COMPLETED! #####\n"
            f"\n\tFollow the guidance provided above and then re-run .build() when you've\n"
            f"\n\tresolved the issues preventing the build from completing."
        )
        
        
    def check_build(self, verbose=True):
        table_exists = check_table_exists(self.full_table_id)
        if table_exists:
            column_names = self.get_column_names()
            person_id_present = "person_id" in column_names
            event_start_present = "event_start_date" in column_names
            event_end_present = "event_end_date" in column_names
        else:
            person_id_present = False
            event_start_present = False
            event_end_present = False
        return table_exists, person_id_present, event_start_present, event_end_present
        
    
    def build(self):
        
        print(f"\t ##### BUILDING FDM TABLE COMPONENTS FOR {self.table_id} #####")
        print("_" * 80 + "\n")

        print(f"1. Copying {self.table_id} to {self.dataset_id}:")
        self.copy_table_to_dataset(user_input=True)

        print(f"\n2. Adding person_id column:")
        person_id_added = self._add_person_id_to_table(user_input=True)
        if not person_id_added:
            print(self._build_not_completed_message)
            return None

        print(f"\n3. Adding event_start_date column:")
        event_start_added = self._add_event_start_date_to_table(
            event_start_date_cols=None, 
            event_start_date_format=None,
            user_input=True
        )
        if not event_start_added:
            print(self._build_not_completed_message)
            return None

        print(f"\n4. Adding event_end_date column:")
        event_end_added = self._add_event_end_date_to_table(
            event_end_date_cols=None, 
            event_end_date_format=None,
            user_input=True
        )
        if not event_start_added:
            print(self._build_not_completed_message)
            return None

        print("_" * 80 + "\n")
        print(f"\t ##### BUILD PROCESS FOR {self.table_id} COMPLETE! #####\n")
    
    
    def quick_build(self, event_start_date_cols, event_start_date_format,
                    event_end_date_cols=None, event_end_date_format=None,
                    verbose=True):
        
        if verbose:
            print(f"Building {self.table_id}:")
        self.copy_table_to_dataset(verbose=verbose)
        self._add_person_id_to_table(verbose=verbose)
        self._add_event_start_date_to_table(
            event_start_date_cols,
            event_start_date_format,
            verbose=verbose
        )
        if event_end_date_cols is not None:
            self._add_event_end_date_to_table(
                event_end_date_cols,
                event_end_date_format,
                verbose=verbose
            )
        else:
            print("    no event_end_date info provided")
        print("Done.")
    
    
    def get_column_names(self):
        
        table = CLIENT.get_table(self.full_table_id)
        return [field.name for field in table.schema]
            
            
    def add_column(self, column_sql):
        sql = f"""
            SELECT *, {column_sql}
            FROM {self.full_table_id}
        """
        run_sql_query(sql, destination=self.full_table_id)
    
    
    def drop_column(self, column):
        sql = f"""
            ALTER TABLE {self.full_table_id}
            DROP COLUMN {column}
        """
        run_sql_query(sql)
    
    
    def rename_columns(self, names_map, verbose=True):
        rename_columns_in_bigquery(table_id=self.full_table_id,
                                   names_map=names_map,
                                   verbose=verbose)
        
        
    def head(self, n=10):
        sql = f"""
            SELECT *
            FROM `{self.full_table_id}`
            LIMIT {n}
        """
        return pd.read_gbq(sql)
                                                                                                          
    
    def copy_table_to_dataset(self, user_input=False, verbose=False):
        
        src_copy_exists = check_table_exists(self.full_table_id)
        
        if not user_input:
            if src_copy_exists:
                if verbose:
                    print(f"    existing copy of {self.table_id} in " 
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
        else:
                    
            fresh_copy = True
            if src_copy_exists:
                response = input(f"""
    A copy of {self.table_id} already exists in {self.dataset_id}. 
    You can continue with the existing {self.table_id} table in {self.dataset_id}
    or make a fresh copy from the source dataset."   
    
    Continue with existing copy?
    > Type y or n: """)
                while response not in ["y", "n"]:
                    response = input("\n\tYour response didn't match y or n."
                                     "\n\t> Try again: ")
                if response == "y":
                    fresh_copy = False

            if fresh_copy:
                sql = f"""
                    SELECT * 
                    FROM `{self.source_table_full_id}`
                """
                try:
                    run_sql_query(sql, destination=self.full_table_id)
                    print(f"\n    Table {self.table_id} copied to "
                          f"{self.dataset_id}!")
                except Exception as ex:
                    print(f"""
    Looks like something went wrong! Likely culprits are:"
    
    1. You misspelled either the source table location or dataset id: 
    
        Source table location - "{self.source_table_full_id}" 
        Dataset id - "{self.dataset_id}" 
        
    If so, just correct the spelling error and then re-initialise.
    
    2. The dataset {self.dataset_id} doesn't exist yet
    
    If so, and you have the relevant permissions, you can create a new dataset
    using an FDMDataset object and .create_dataset(), or just use GCP.
    Otherwise, if you don't have the necessary permissions, have a word with  
    the CYP data team and have them create you a dataset.
    
    Note: DO NOT CONTINUE TO USE THIS PARTICULAR FDMTable INSTANCE! If you do, 
    you're going to see a whole bunch more error messages!
    
    Full error message is as follows:
                    """)
                    raise ex
            else:
                print(f"\n    Continuing with existing copy of {self.table_id}")
            
            
    def _add_person_id_to_table(self, user_input=False, verbose=False):
        
        
        col_names = self.get_column_names()
        
        # find matching identifier columns and correct syntax if required
        correct_identifiers = ["person_id", "digest", "EDRN"]
        identifiers_in_src = [col for col in self.get_column_names()
                              if col in correct_identifiers] 
        if not identifiers_in_src:
            if not user_input:
                raise ValueError(
                    f"None of {correct_identifiers} in table columns"
                )
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
                return False
            else:
                miss_named_id_col = response 
                response = input(f"""
    Does {miss_named_id_col} contain person_ids, digests or EDRNs?
    > Type either person_id, digest or EDRN: 
                """)
                while response not in ["person_id", "digest", "EDRN"]:
                    response = input(f"""
    Response needs to match one of person_id, digest or EDRN and is 
    case-sensitive.
    > Response:
                    """)
                self.rename_columns({miss_named_id_col: response})
        
        if "person_id" in self.get_column_names():
            if user_input or verbose:
                print(f"    {self.table_id} already contains person_id column")
            return True
        else:
            identifier = [col for col in self.get_column_names()
                          if col in ["digest", "EDRN"]][0]
            sql = f"""
                SELECT demo.person_id, src.*
                FROM `{self.full_table_id}` src
                LEFT JOIN `{DEMOGRAPHICS}` demo
                ON src.{identifier} = demo.{identifier}
            """
            run_sql_query(sql, destination=self.full_table_id)
            if user_input or verbose:
                print("    person_id column added")
            return True
            
            
    def _get_event_date_df(self, date_cols, yearfirst, dayfirst):

        table = CLIENT.get_table(self.full_table_id)
        col_data = {field.name: field.field_type 
                    for field in table.schema}
        if type(date_cols) == list and len(date_cols) == 3:
            cast_cols_sql = []
            for col in date_cols:
                if col in col_data.keys() and col_data[col] == "STRING":
                    cast_cols_sql.append(col)
                elif col in col_data.keys(): 
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
        
        date_format_settings = {
            "YMD": [True, False],
            "YDM": [True, True],
            "DMY": [False, True],
            "MDY": [False, False]
        }

        if "uuid" not in self.get_column_names():
            add_uuid_sql = f"""
                SELECT GENERATE_UUID() AS uuid, *
                FROM `{self.full_table_id}`
            """
            run_sql_query(add_uuid_sql, destination=self.full_table_id)

        yearfirst, dayfirst = date_format_settings[date_format]
        dates_df = self._get_event_date_df(date_cols, 
                                           yearfirst=yearfirst,
                                           dayfirst=dayfirst)
        
        if dates_df.parsed_date.isna().all():
            self.drop_column("uuid")
            return False
        
        temp_dates_id = f"{PROJECT}.{self.dataset_id}.tmp_dates"
        dates_df.to_gbq(destination_table=temp_dates_id,
                        project_id=PROJECT,
                        table_schema=[{"name":"parsed_date", "type":"DATETIME"}],
                        if_exists="replace")

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
    
    
    def _add_event_start_date_to_table(self, event_start_date_cols, 
                                       event_start_date_format, 
                                       user_input=False, verbose=False):
        
        if "event_start_date" in self.get_column_names():
            if not user_input:
                if verbose:
                    print(f"    event_start_date column already present")
                return None
            response = input(f"""
    event_start_date column is already present.
    
    You can continue with the existing event_start_date column or rebuild
    a new event_start_date_column from scratch.
    
    Continue with existing event_start_date? 
    > Type y or n: """)
            while response not in ["y", "n"]:
                response = input("    Your response didn't match y or n.\n"
                                 "    > Try again: ")
            if response == "y":
                return True
            else:
                self.drop_column("event_start_date")
        
        if user_input:
            single_col_y_n = input(f"""
    An event start date is required to build the observation_period table. This 
    information should be contained within one or more columns of your table. 
    If unsure a quick look at the table data in BigQuery should clarify.
    
    To start, is the event start data found in one column that can be easily 
    parsed with a day, month and year?
    > Type y or n """)
            while single_col_y_n not in ["y", "n"]:
                single_col_y_n = input("    Your response didn't match y or n.\n"
                                       "    > Try again: ")
            if single_col_y_n == "y":
                event_start_date_cols = input("""
    Which column contains the event start date?
    > Type the name (case sensitive): """)
                while event_start_date_cols not in self.get_column_names():
                    event_start_date_cols = input(f"""
    {event_start_date_cols} doesn't match any of the columns in {self.table_id}"
    > Try again: """)
                event_start_date_format = input("""
    What format does the date appear in YMD/YDM/DMY/MDY?
    > Type one: """)
                while event_start_date_format not in ["YMD", "YDM", "DMY", "MDY"]:
                    event_start_date_cols = input("""
    Response must be one of YMD/YDM/DMY/MDY."
    > Try again: """)
            else:
                year = input("""
    We'll build the event start date beginning with the year. Where can the
    year information be found?

    Your response can be the name of a column that contains the year (year only,
    other formats can't be parsed) or a static value (e.g. 2022).

    If the year information isn't contained in one column, type quit as your 
    response, add a column with the year information and then re-run .build(). 
    You may find the .add_column() method useful for this.

    > Response: """)
                if year == "quit":
                    return False
                month = input("""
    And now we'll move onto the month. The same guidance as above applies.
    Remebmer, a static value like 02, or Feb, or February is acceptable.

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
                event_start_date_cols = [year, month, day]
                event_start_date_format = "YMD"
                print("\n    adding event_start_date_column...")
                
        dates_parsed = self._add_parsed_date_to_table(
            date_cols=event_start_date_cols,  
            date_format=event_start_date_format,  
            date_column_name="event_start_date"
        )
        if dates_parsed:
            if user_input or verbose:
                print("    event_start_date column added")
            return True
        elif user_input:
            print("""
    Looks like something went wrong and the parser couldn't understand the date
    information you provided. Check your responses above and re-run .build() if
    you notice any errors. Otherwise, seek help from the CYP data team.""")
            return False
        else:
            raise ValueError("event_start_dates couldn't be parsed - all values None")
        
        
    def _add_event_end_date_to_table(self, event_end_date_cols, 
                                     event_end_date_format, 
                                     user_input=False, verbose=False):
        
        if "event_end_date" in self.get_column_names():
            if not user_input:
                if verbose:
                    print(f"    event_start_date column already present")
                return None
            response = input("""
    event_end_date column is already present.

    You can continue with the existing event_end_date column or rebuild a new 
    event_end_date_column from scratch.

    Continue with existing event_end_date?
    > Type y or n: """)
            while response not in ["y", "n"]:
                response = input("\n    Your response didn't match y or n."
                                 "\n    > Try again: ")
            if response == "y":
                return True
            else:
                self.drop_column("event_end_date")
        
        if user_input: 
            has_event_end_date = input("""
    An event end date may or may not be relevant to this source data. For example, 
    hospital visits or academic school years have an end date as well as a start 
    date.

    If you're unsure weather or not the source data should include an event end 
    date, seek help from the CYP data team."

    Does this data have an event end date?"
    > Type y or n: """)
            
            while has_event_end_date not in ["y", "n"]:
                has_event_end_date = input("\n    Your response didn't match y or n."
                                       "\n    > Try again: ")
                
            if not has_event_end_date == "y":
                return True
            
            single_col_y_n = input("""
    The process will now proceed in exactly the same way as with the event start 
    date. Refer to the guidance above if at all unsure about the responses to any
    of the following questions.

    Is the event end data found in one column that can be easily parsed with a 
    day, month and year?
    > Type y or n: """)
            while single_col_y_n not in ["y", "n"]:
                single_col_y_n = input("\n    Your response didn't match y or n."
                                       "\n    > Try again: ")
                
            if single_col_y_n == "y":
                event_end_date_cols = input(
                    "\n    Which column contains the event end date."
                    "\n    > Type the name (case sensitive): "
                )
                while event_end_date_cols not in self.get_column_names():
                    event_end_date_cols = input(f"""
    {event_end_date_cols} doesn't match any of the columns in {self.table_id}"
    > Try again: """)
                event_end_date_format = input("""
    What format does the date appear in? YMD/YDM/DMY/MDY
    > type one: """)
                while event_end_date_format not in ["YMD", "YDM", "DMY", "MDY"]:
                    event_end_date_cols = input("""
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
                event_end_date_cols = [year, month, day]
                event_end_date_format = "YMD"
                
        dates_parsed = self._add_parsed_date_to_table(
            date_cols=event_end_date_cols,  
            date_format=event_end_date_format,  
            date_column_name="event_end_date"
        )
        if dates_parsed:
            if user_input or verbose:
                print("    event_end_date column added")
                return True
        elif user_input:
            print("""
    Looks like something went wrong and the parser couldn't understand the date
    information you provided. Check your responses above and re-run .build() if
    you notice any errors. Otherwise, seek help from the CYP data team. """)
            return False
        else:
            raise ValueError("event_end_dates couldn't be parsed - all values None")
