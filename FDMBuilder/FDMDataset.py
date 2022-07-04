# from google.cloud import bigquery
from FDMBuilder.FDMTable import *
    
    
class FDMDataset:
    """A Tool for building an FDM dataset
    
    Takes a dataset with pre-prepared tables and generates the following tables:
    
    1. person table
    2. observation period table
    3. problem tables for each of the source tables (these contain any entries 
       removed for various issues - observations outside birth/death dates etc.)
    4. data_dict for each source table
    
    Args:
        dataset_id: string, id of the dataset in GCP
        
    Attributes:
        dataset_id = id of dataset where table is to be built in GCP
        person_table_id = full id of person table 
        observation_period_table_id = full id of observation_period table
    """
    def __init__(self, dataset_id):
        self.dataset_id = dataset_id
        self.person_table_id = f"{PROJECT}.{dataset_id}.person"
        self.observation_period_table_id = f"{PROJECT}.{dataset_id}.observation_period"
        if not check_dataset_exists(self.dataset_id):
            print(f"Dataset {self.dataset_id} doesn't yet exist!\n\n"
                  "Double-check that you've got the correct spelling. If you wish to\n"
                  "create a new dataset with that name (and you have the relevant permissions)\n"
                  "run .create_dataset()")
    
    
    def build(self, extract_end_date, excluded_tables=[], 
              includes_pre_natal=False):
        """Builds the FDM dataset
        
        Simply requires that the dataset specified when initialising the 
        FDMDataset contains all the required source tables and that the source 
        tables have been "built" using the FDMTable tool. Running .build() then
        generates all the necessary standard FDM tables.
        
        Args:
            includes_pre_natal: bool (default False), determines if observations 
                dated within pre-natal period before birth (300 days) are 
                removed, False, or kept, True,  when generating the problem 
                tables
        
        Returns:
            None - all changes in GCP
        """
        
        print(f"\t\t ##### BUILDING FDM DATASET {self.dataset_id} #####")
        print("_" * 80 + "\n")
        print("1. Checking dataset for source tables:\n")
        build_ready = self._get_fdm_tables(excluded_tables)
        if not build_ready:
            print(
            "_" * 80 + "\n\n"  
            f"\t ##### BUILD PROCESS FOR {self.dataset_id} COULD NOT BE COMPLETED! #####\n"
            f"\tFollow the guidance provided above and then re-run .build() when you've\n"
            f"\tresolved the issues preventing the build from completing."
            )
            return None
        print("\n2. Building person table\n")
        self._build_person_table()
        print("3. Separating out problem entries from source tables\n")
        self._split_problem_entries_from_src_tables(extract_end_date, 
                                                    includes_pre_natal)
        print("\n4. Rebuilding person table\n")
        self._build_person_table()
        print("5. Building observation_period table\n")
        self._build_observation_period_table()
        print("6. Building data dictionaries\n")
        self._build_data_dictionaries()
        print("_" * 80 + "\n")
        print(f"\t ##### BUILD PROCESS FOR {self.dataset_id} COMPLETE! #####\n")
        
    
    def create_dataset(self):
        """Creates dataset named in dataset_id if it doesn't already exist
        
        Returns: 
            None - all changes in GCP
        """
        try:
            CLIENT.get_dataset(self.dataset_id)
            print(f"Dataset {self.dataset_id} already exists!")
        except:
            dataset = bigquery.Dataset(f"{PROJECT}.{self.dataset_id}")
            dataset.location = "europe-west2"
            CLIENT.create_dataset(dataset, timeout=30)
            print(f"Dataset {self.dataset_id} created")
        
    
    def _get_fdm_tables(self, excluded_tables):
        """Generates FDMTable objects for every source table in dataset
            
        Collects all non-standard FDM tables in dataset i.e. the source datasets,
        checks if they're ready for an FDM build (see `.check_build()` method of
        FDMTable for more info) stores FDMTable objects for each as a list in a 
        `tables` attribute, for use in rest of build process.
        
        Returns:
            bool, True if all tables are ready for FDM build, otherwise False
        """
              
        standard_tables = ["person", "observation_period"]
        fdm_src_tables = []
        build_ready = True
        for table in CLIENT.list_tables(self.dataset_id):
            is_standard_table = table.table_id in standard_tables
            is_problem_table = "fdm_problems" in table.table_id
            is_data_dict = "data_dict" in table.table_id
            is_excluded = table.table_id in excluded_tables
            if is_standard_table or is_problem_table or is_data_dict or is_excluded:
                continue
            fdm_table = FDMTable(
                source_table_id = (f"{self.dataset_id}.{table.table_id}"),
                dataset_id = self.dataset_id
            )
            (exists, has_person_id, person_id_is_int, has_fdm_start, 
             has_fdm_end, has_problem_table) = fdm_table.check_build()
            if not np.all(has_person_id, person_id_is_int, has_fdm_start):
                person_missing = ("\n\t* no person_id column present" 
                                  if not has_person_id else "")
                person_not_int = ("\n\t* person_id column is not INTEGER format" 
                                  if not has_person_id 
                                  and not person_id_is_int else "")
                start_missing = ("\n\t* no fdm_start_date column present" 
                                 if not has_fdm_start else "")
                errors = person_missing + person_not_int + start_missing
                print(f"""
    {table.table_id} is not ready for dataset build:{errors}
    
    Complete the table build process for {table.table_id} and then re-run the
    dataset build -- OR -- if the table doesn't apply to the usual FDM criteria
    e.g. it's a lookup table, then add to the `excluded_tables` argument of 
    `.build()`.
                """)
                build_ready = False
            else:
                if has_problem_table:
                    fdm_table.recombine()
                fdm_end = ' fdm_end_date' if has_fdm_end else ''
                print(f"    * {table.table_id} contains: "
                      f" - person_id - fdm_start_date {fdm_end}"
                      "\n\t-> Table ready")
            fdm_src_tables.append(fdm_table)
        self.tables = fdm_src_tables
        return build_ready
                
                
    def _build_person_table(self):
        """Builds person table for dataset
        
        Collects all the unique person_ids in each of the source tables and 
        generates a copy of the master person table with entries that match 
        these ids. If a person table already exists, a fresh table is built and 
        overwrites the existing person table.
        
        Returns:
            None - all changes in GCP
        """
        # generate new table with unique person ids
        person_id_union_sql = "\nUNION ALL\n".join(
            [f"SELECT person_id FROM `{table.full_table_id}`"
             for table in self.tables]
        )
        person_ids_sql = f"""
            WITH person_ids AS (
                {person_id_union_sql}
            )
            SELECT DISTINCT person_id
            FROM person_ids
        """
        run_sql_query(person_ids_sql, destination=self.person_table_id) 
        
        # join columns from master person table in query
        full_person_table_sql = f"""
            SELECT a.person_id, b.* EXCEPT(person_id)
            FROM `{self.person_table_id}` a
            INNER JOIN `{MASTER_PERSON}` b
            ON a.person_id = b.person_id
            ORDER BY person_id
        """
        person_bq_table = run_sql_query(full_person_table_sql,  
                                        destination=self.person_table_id)
        
        print(f"    * Person table built with {person_bq_table.num_rows} "
              "entries\n")
        
    
    def _build_observation_period_table(self):
        """Builds the observation period table
        
        Creates a union of the start/end dates in all the source tables and 
        calculates a MIN start date and MAX end date for each unique person_id. 
        The process assumes all the error entries have already been removed 
        (see _split_problem_entries_from_src_tables)
        
        Returns:
            None - all changes in GCP
        """
        full_union_sql_list = []
        for table in self.tables:
            no_end_date = "fdm_end_date" not in table.get_column_names()
            union_sql = f"""
                SELECT person_id, fdm_start_date, 
                    {"fdm_start_date AS fdm_end_date"
                     if no_end_date else "fdm_end_date"}
                FROM `{table.full_table_id}`  
                WHERE person_id IS NOT NULL
            """
            full_union_sql_list.append(union_sql)
                
        full_union_sql = "\nUNION ALL\n".join(full_union_sql_list)
            
        observation_period_sql = f"""
            WITH all_src_dates AS (
                {full_union_sql}
            )
            SELECT person_id, 
                MIN(fdm_start_date) AS observation_period_start_date,
                MAX(fdm_end_date) AS observation_period_end_date 
            FROM all_src_dates
            GROUP BY person_id
        """
        obs_bq_table = run_sql_query(observation_period_sql, 
                                     destination=self.observation_period_table_id)
        
        print(f"    * observation_period table built with {obs_bq_table.num_rows} "
              "entries\n")
        
        
    def _build_data_dictionaries(self):
        """Builds a data dict in GCP for each source table
        
        Simply takes all the tables in the `tables` attribute and calls the 
        `build_data_dict` method for each 
        
        Returns:
            None - all changes in GCP
        """
        for table in self.tables:
            table.build_data_dict()
            print(f"    * {table.table_id}_data_dict built")
        
        
    def _add_problem_entries_column_to_table(self, table, extract_end_date, 
                                             includes_pre_natal):
        """Labels all problem entries in a table
        
        Creates a "problems" column in the input table and labels any entries 
        that have a "problem" - problems include:
        
        * No person_id
        * person_id doesn't appear in master person table
        * event date before birth date
        * event date after death date (+42 days)
        
        and so on - read the code for the full list of "problems"
        
        Args:
            table: FDMTable, table to which problems column is added
            includes_pre_natal: bool, if the entries with a date within the 
                pre-natal period should be marked as problems, True, the 
                pre-natal entries are left blank, False, they are marked as 
                problems.
                
        Returns:
            None - all changes in GCP
        """
        if "fdm_problem" in table.get_column_names():
            print(f"\tfdm_problem column already exists in {table.table_id}."
                  " Dropping...")
            table.drop_column("fdm_problem")
         
        no_person_id = "person_id IS NULL"
        person_id_not_in_master = f"""
            NOT EXISTS(
                SELECT person_id
                FROM `{self.person_table_id}` as person
                WHERE person.person_id = src.person_id
            )
        """
        person_has_no_dob = f"""
            EXISTS(
                SELECT person_id
                FROM `{self.person_table_id}` as person
                WHERE person.person_id = src.person_id
                    AND person.birth_datetime IS NULL
            )
        """
        no_fdm_start_date = "fdm_start_date is NULL"
        fdm_start_before_pre_natal_period = f"""
            EXISTS(
                SELECT birth_datetime
                FROM `{self.person_table_id}` AS person
                WHERE src.person_id = person.person_id 
                    AND DATETIME_ADD(
                        src.fdm_start_date, 
                        INTERVAL 294 DAY) < person.birth_datetime
            )
        """       
        fdm_start_after_death = f"""
            EXISTS(
                SELECT death_datetime
                FROM `{self.person_table_id}` AS person
                WHERE src.person_id = person.person_id 
                    AND person.death_datetime IS NOT NULL
                    AND src.fdm_start_date > DATETIME_ADD(person.death_datetime,
                                                            INTERVAL 42 DAY)
            )
        """
        fdm_start_after_extract_end = f"""
            EXISTS(
                SELECT fdm_start_date
                FROM `{self.person_table_id}` AS person
                WHERE src.person_id = person.person_id 
                    AND src.fdm_start_date > CAST("{extract_end_date}" AS DATETIME)
            )
        """
        messages_with_problem_cases = {
            "Entry has no person_id": no_person_id,
            "person_id isn't in master person table": person_id_not_in_master,
            "person has no bith_datetime in master person table": person_has_no_dob,
            "Entry has no fdm_start_date": no_fdm_start_date,
            "fdm_start_date is before person birth_datetime": 
            fdm_start_before_pre_natal_period,
            "fdm_start_date is after death_datetime (+42 days)": 
            fdm_start_after_death,
            "fdm_start_date is after the end date for the data extract":
            fdm_start_after_extract_end
        }
        if "fdm_end_date" in table.get_column_names():
            no_fdm_end_date = "fdm_end_date is NULL"
            end_before_start = "fdm_end_date < fdm_start_date"
            fdm_end_before_birth = f"""
                EXISTS(
                    SELECT birth_datetime
                    FROM `{self.person_table_id}` AS person
                    WHERE src.person_id = person.person_id 
                        AND src.fdm_end_date < person.birth_datetime
                )
            """
            fdm_end_after_death = f"""
                EXISTS(
                    SELECT death_datetime
                    FROM `{self.person_table_id}` AS person
                    WHERE src.person_id = person.person_id 
                        AND person.death_datetime IS NOT NULL
                        AND src.fdm_end_date > DATETIME_ADD(person.death_datetime, 
                                                              INTERVAL 42 DAY)
                )
            """
            fdm_end_after_extract_end = f"""
                EXISTS(
                    SELECT fdm_end_date
                    FROM `{self.person_table_id}` AS person
                    WHERE src.person_id = person.person_id 
                        AND src.fdm_end_date > CAST("{extract_end_date}" AS DATETIME)
                )
            """
            messages_with_problem_cases[
                "Entry has no fdm_end_date"
            ] = no_fdm_end_date
            messages_with_problem_cases[
                "fdm_end_date is before fdm_start_date"
            ] = end_before_start
            messages_with_problem_cases[
                "fdm_end_date is before person birth_datetime" 
            ] = fdm_end_before_birth
            messages_with_problem_cases[
                "fdm_end_date is after person death_datetime"
            ] = fdm_end_after_death
            messages_with_problem_cases[
                "fdm_end_date is after extract end date"
            ] = fdm_end_after_extract_end

        if not includes_pre_natal:
            fdm_start_in_pre_natal_period = f"""
                EXISTS(
                    SELECT birth_datetime
                    FROM `{self.person_table_id}` AS person
                    WHERE src.person_id = person.person_id 
                        AND src.fdm_start_date < person.birth_datetime
                        AND DATETIME_ADD(
                            src.fdm_start_date, 
                            INTERVAL 300 DAY) >= person.birth_datetime
                )
            """       
            messages_with_problem_cases[
                "fdm_start_date is before person birth_datetime - Note: Within pre-natal period" 
            ] = fdm_start_in_pre_natal_period

        problem_col_cases = ("CASE " + " ".join([
            f'WHEN {problem_sql} THEN "{problem_text}"'
            for problem_text, problem_sql in messages_with_problem_cases.items()
        ]) + ' ELSE "No problem" END')

        problem_tab_sql = f"""
            SELECT {problem_col_cases} AS fdm_problem, *
            FROM `{table.full_table_id}` AS src
            ORDER BY person_id
        """

        run_sql_query(problem_tab_sql, destination=table.full_table_id)
            
            
    def _split_problem_entries_from_src_tables(self,  extract_end_date, 
                                               includes_pre_natal):
        """Splits source tables into those with/without problems
        
        Takes each source table with a problems column, and separates the 
        entries that are marked with a problem into a separate 
        [source-table-name]_problems table.
        
        Args:
            includes_pre_natal: bool, weather entries daten in pre-natal period 
                i.e. after conception but prior to birth should be counted 
                as problems or not. True, pre-natal events aren't problems, 
                False, they are.
                
        Returns:
            None - all changes in GCP
        """
        for table in self.tables:

            print(f"    {table.table_id}:")
            self._add_problem_entries_column_to_table(table,
                                                      extract_end_date, 
                                                      includes_pre_natal)
            problem_table_sql = f"""
                SELECT * FROM `{table.full_table_id}`
                WHERE fdm_problem != "No problem"
                ORDER BY person_id
            """
            problem_table_id = f"{table.full_table_id}_fdm_problems"
            problem_bq_table = run_sql_query(problem_table_sql, 
                                             destination=problem_table_id)
            print(f"\t* {problem_bq_table.num_rows} problem entries identified "
                  f"and removed to {table.table_id}_fdm_problems")

            src_table_sql = f"""
                SELECT * EXCEPT(fdm_problem) FROM `{table.full_table_id}`
                WHERE fdm_problem = "No problem"
                ORDER BY person_id
            """
            src_bq_table = run_sql_query(src_table_sql, 
                                         destination=table.full_table_id)
            print(f"\t* {src_bq_table.num_rows} entries remain in {table.table_id}")
            
        