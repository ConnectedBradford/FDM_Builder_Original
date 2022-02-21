# from google.cloud import bigquery
from FDMTable import *
    
    
class FDMDataset:
    
    
    def __init__(self, dataset_id, fdm_tables):
        self.dataset_id = dataset_id
        self.person_table_id = f"{PROJECT}.{dataset_id}.person"
        self.observation_period_table_id = f"{PROJECT}.{dataset_id}.observation_period"
        dataset_exists = self._check_dataset_exists()
        if not dataset_exists:
            print(f"Dataset {self.dataset_id} doesn't yet exist!\n\n"
                  "Double-check that you've got the correct spelling. If you wish to\n"
                  "create a new dataset with that name (and you have the relevant permissions)\n"
                  "run .create_dataset()")
        self.tables = fdm_tables
    
    
    def build(self):
        
        print(f"\t\t ##### BUILDING FDM DATASET {self.dataset_id} #####")
        print("_" * 80 + "\n")
        self._check_fdm_tables()
        print("\n2. Building person table:\n")
        self._build_person_table()
        self._build_missing_person_ids()
        self._build_person_ids_missing_from_master()
        print("5. Building initial observation_period table\n")
        self._build_observation_period_table()
        self._remove_entries_outside_observation_period()
        print("\n7. Rebuilding person table:\n")
        self._build_person_table()
        print("8. Rebuilding observation_period table\n")
        self._build_observation_period_table()
        print("_" * 80 + "\n")
        print(f"\t ##### BUILD PROCESS FOR {self.dataset_id} COMPLETE! #####\n")
        
    
    def create_dataset(self):
        try:
            CLIENT.get_dataset(self.dataset_id)
            print(f"Dataset {self.dataset_id} already exists!")
        except:
            dataset = bigquery.Dataset(f"{PROJECT}.{self.dataset_id}")
            dataset.location = "europe-west2"
            CLIENT.create_dataset(dataset, timeout=30)
            print(f"Dataset {self.dataset_id} created")
        
    
    def _check_dataset_exists(self):
        try:
            CLIENT.get_dataset(self.dataset_id)
            return True
        except:
            return False
        
    
    def _check_fdm_tables(self):
              
        print("1. Checking source input tables:\n")
        
        for table in self.tables:
            
            if not type(table) is FDMTable:
                raise ValueError(
                    f"\t{table} is not an FDM table. All inputs must be built FDM tables."
                    "\n\tCheck and re-initialise FDMDataset with correct input."
                )
            elif not "person_id" in  table.get_column_names():
                raise ValueError(
                    "aint no person_id"
                )
            elif not "event_start_date" in  table.get_column_names():
                raise ValueError(
                    "aint no event_start_date"
                )
            elif not table.dataset_id == self.dataset_id:
                raise ValueError(
                    f"wrong dataset for {table.table_id} - {table.dataset_id}"
                )
            else:
                print(f"\t* {table.table_id} - OK")
              
        
    def _build_person_table(self):
        
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
        print(f"\t* Joining data from master person table")
        full_person_table_sql = f"""
            SELECT a.person_id, b.* EXCEPT(person_id)
            FROM `{self.person_table_id}` a
            INNER JOIN `{MASTER_PERSON}` b
            ON a.person_id = b.person_id
        """
        run_sql_query(full_person_table_sql, 
                      destination=self.person_table_id)
        print("\t* Person table built!\n")
        
    
    def _build_missing_person_ids(self):
        
        print("3. Building table of individuals with no person_id\n")
        select_queries = [
            f"""
                SELECT "{identifier}" AS identifier, {identifier} AS value 
                FROM `{table.full_table_id}` 
                WHERE person_id IS NULL 
            """ 
            for table in self.tables 
            for identifier in table.get_identifier_columns() 
            if identifier != "person_id"
        ]
        if select_queries:
            union_query = "\nUNION ALL\n".join(select_queries)
            sql = f"""
                WITH missing_person_ids AS (
                    {union_query}
                )
                SELECT *
                FROM missing_person_ids
                GROUP BY identifier, value
            """
            print(sql)
            table_id = f"{PROJECT}.{self.dataset_id}.individuals_missing_person_id"
            run_sql_query(sql, destination=table_id) 
            tab = CLIENT.get_table(table_id)
            print(f"\t* individuals_missing_person_id created with {tab.num_rows} entries\n")
        else:
            print(f"\t* all entries have a person_id\n")
    
            
    def _build_person_ids_missing_from_master(self):
        
        print("4. Building person_ids missing from master table\n")
        # generate new table with unique person ids
        person_id_union_sql = "\nUNION ALL\n".join(
            [f"SELECT person_id FROM `{table.full_table_id}`"
             for table in self.tables]
        )
        missing_ids_sql = f"""
            WITH all_person_ids AS (
                {person_id_union_sql}
            )
            SELECT DISTINCT person_id
            FROM all_person_ids
            WHERE NOT EXISTS(
                SELECT person_id
                FROM `{self.person_table_id}` person_table
                WHERE all_person_ids.person_id = person_table.person_id
            )
            AND person_id IS NOT NULL
        """
        table_id = f"{PROJECT}.{self.dataset_id}.person_ids_missing_from_masater"
        run_sql_query(missing_ids_sql, destination=table_id) 
        
        tab = CLIENT.get_table(table_id)
        print(f"\t* person_ids_missing_from_master created with {tab.num_rows} entries\n")
        
        
    def _build_observation_period_table(self):
        
        full_union_sql_list = []
        for table in self.tables:
            no_end_date = "event_end_date" not in table.get_column_names()
            union_sql = f"""
                SELECT person_id, event_start_date, 
                    {"event_start_date AS event_end_date"
                     if no_end_date else "event_end_date"}
                FROM `{table.full_table_id}`  
                WHERE person_id IS NOT NULL
            """
            full_union_sql_list.append(union_sql)
                
        full_union_sql = "\nUNION ALL\n".join(full_union_sql_list)
            
        observation_period_sql = f"""
            WITH possible_dates AS (
                WITH all_src_dates AS (
                    {full_union_sql}
                )
                SELECT person_id, 
                    MIN(event_start_date) AS possible_start_date,
                    MAX(event_end_date) AS possible_end_date 
                FROM all_src_dates
                GROUP BY person_id
            
                UNION ALL
            
                SELECT person_id,
                    birth_datetime AS possible_start_date, 
                    DATETIME_ADD(IFNULL(death_datetime, 
                                        DATETIME "9999-01-01 00:00:00"), 
                                 INTERVAL 42 DAY) as possible_end_date 
                FROM `{self.person_table_id}`
            )
            SELECT person_id, 
            MAX(possible_start_date) AS observation_period_start_date,
            MIN(possible_end_date) AS observation_period_end_date
            FROM possible_dates
            GROUP BY person_id 
        """
        run_sql_query(observation_period_sql,
                      destination=self.observation_period_table_id)
        
        print(f"\t* observation_period table built\n")
        
        
    def _remove_entries_outside_observation_period(self):
        
        print("6. Removing entries outside observation period\n")
        for table in self.tables:
            no_end_date = "event_end_date" not in table.get_column_names()
            table_plus_obs_sql = f"""
                SELECT a.*, 
                    {"a.event_start_date AS event_end_date,"
                     if no_end_date else ""}
                    b.observation_period_start_date, 
                    b.observation_period_end_date
                FROM `{table.full_table_id}` AS a
                INNER JOIN `{self.observation_period_table_id}` AS b
                ON a.person_id = b.person_id
            """
            error_entries_conditions = f"""
                event_start_date < observation_period_start_date 
                OR event_start_date > observation_period_end_date 
                OR event_end_date > observation_period_end_date
            """
            error_entries_sql = f"""
                WITH table_plus_obs AS (
                    {table_plus_obs_sql}
                )
                SELECT * EXCEPT(observation_period_start_date, 
                                observation_period_end_date
                                {", event_end_date)" if no_end_date
                                 else ")"}
                FROM table_plus_obs
                WHERE {error_entries_conditions}
            """
            error_table_id = f"{PROJECT}.{self.dataset_id}.{table.table_id}_outside_obs"
            run_sql_query(error_entries_sql, destination=error_table_id)
            non_error_entries_sql = f"""
                WITH table_plus_obs AS (
                    {table_plus_obs_sql}
                )
                SELECT * EXCEPT(observation_period_start_date,
                                observation_period_end_date
                                {", event_end_date)" if no_end_date
                                 else ")"}
                FROM table_plus_obs
                WHERE NOT({error_entries_conditions})
            """
            run_sql_query(non_error_entries_sql, destination=table.full_table_id)
            print(f"\t* entries outside observation period removed from {table.table_id}")
            print(f"\t  and stored in {table.table_id}_outside_obs")
        
