# from google.cloud import bigquery
from FDMBuilder.FDMTable import *
    
    
class FDMDataset:
    
    
    def __init__(self, dataset_id):
        self.dataset_id = dataset_id
        self.person_table_id = f"{PROJECT}.{dataset_id}.person"
        self.observation_period_table_id = f"{PROJECT}.{dataset_id}.observation_period"
        if not check_dataset_exists(self.dataset_id):
            print(f"Dataset {self.dataset_id} doesn't yet exist!\n\n"
                  "Double-check that you've got the correct spelling. If you wish to\n"
                  "create a new dataset with that name (and you have the relevant permissions)\n"
                  "run .create_dataset()")
    
    
    def build(self):
        
        print(f"\t\t ##### BUILDING FDM DATASET {self.dataset_id} #####")
        print("_" * 80 + "\n")
        build_ready = self._get_fdm_tables()
        self.add_problem_entries_back_into_src_tables()
        print("\nBuilding person table\n")
        self._build_person_table()
        self._split_problem_entries_from_src_tables()
        print("\nRebuilding person table\n")
        self._build_person_table()
        print("Building observation_period table\n")
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
        
    
    def _get_fdm_tables(self):
              
        print("Checking dataset for source tables:\n")
        
        standard_tables = ["person", "observation_period"]
        fdm_src_tables = []
        for table in CLIENT.list_tables(self.dataset_id):
            if table.table_id in standard_tables or "problems" in table.table_id:
                continue
            fdm_table = FDMTable(
                source_table_full_id = (f"{PROJECT}.{self.dataset_id}"
                                        f".{table.table_id}"),
                dataset_id = self.dataset_id
            )
            (exists, has_person_id, 
             has_event_start, has_event_end) = fdm_table.check_build()
            if not has_person_id or not has_event_start:
                person_missing = "* person_id " if not has_person_id else ""
                start_missing = "* event_start_date " if not has_event_start else ""
                print(f"""
    {table.table_id} is missing: {person_missing}{start_missing}
    
    Complete the table build process for {table.table_id} and then re-run the
    dataset build.
                """)
                return False
            else:
                event_end = ' * event_end_date' if has_event_end else ''
                print(f"    {table.table_id} contains: "
                      f" * person_id * event_start_date {event_end}"
                      "\n\t-> Table ready")
            fdm_src_tables.append(fdm_table)
        self.tables = fdm_src_tables
                
                
    def add_problem_entries_back_into_src_tables(self):
        problem_table_ids = [
            table.table_id[:-9] 
            for table in CLIENT.list_tables(self.dataset_id)
            if "problems" in table.table_id 
        ]
        if problem_table_ids:
            print(f"\nAdding problem entries back into tables:\n")
            for table_id in problem_table_ids:
                print(f"    {table_id}", end=" ")
                full_table_id = f"{PROJECT}.{self.dataset_id}.{table_id}"
                union_sql = f"""
                    SELECT * 
                    FROM `{full_table_id}`
                    UNION ALL
                    SELECT * EXCEPT(problem)
                    FROM `{full_table_id + "_problems"}`
                """
                run_sql_query(sql=union_sql, destination=full_table_id)
                CLIENT.delete_table(
                    f"{PROJECT}.{self.dataset_id}.{table_id}_problems", 
                    not_found_ok=False
                )
                print("- done")
        
        
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
            WITH all_src_dates AS (
                {full_union_sql}
            )
            SELECT person_id, 
                MIN(event_start_date) AS observation_period_start_date,
                MAX(event_end_date) AS observation_period_end_date 
            FROM all_src_dates
            GROUP BY person_id
        """
        obs_bq_table = run_sql_query(observation_period_sql, 
                                     destination=self.observation_period_table_id)
        
        print(f"    * observation_period table built with {obs_bq_table.num_rows} "
              "entries\n")
        
        
    def _add_problem_entries_column_to_table(self, table):

         
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
        no_event_start_date = "event_start_date is NULL"
        event_start_in_pre_natal_period = f"""
            EXISTS(
                SELECT birth_datetime
                FROM `{self.person_table_id}` AS person
                WHERE src.person_id = person.person_id 
                    AND src.event_start_date < person.birth_datetime
                    AND DATETIME_ADD(
                        src.event_start_date, 
                        INTERVAL 300 DAY) >= person.birth_datetime
            )
        """       
        event_start_before_pre_natal_period = f"""
            EXISTS(
                SELECT birth_datetime
                FROM `{self.person_table_id}` AS person
                WHERE src.person_id = person.person_id 
                    AND DATETIME_ADD(
                        src.event_start_date, 
                        INTERVAL 294 DAY) < person.birth_datetime
            )
        """       
        event_start_after_death = f"""
            EXISTS(
                SELECT death_datetime
                FROM `{self.person_table_id}` AS person
                WHERE src.person_id = person.person_id 
                    AND person.death_datetime IS NOT NULL
                    AND src.event_start_date > DATETIME_ADD(person.death_datetime,
                                                            INTERVAL 42 DAY)
            )
        """
        messages_with_problem_cases = {
            "Entry has no person_id": no_person_id,
            "person_id isn't in master person table": person_id_not_in_master,
            "person has no bith_datetime in master person table": person_has_no_dob,
            "Entry has no event_start_date": no_event_start_date,
            "event_start_date is before person birth_datetime - Note: Within pre-natal period": 
            event_start_in_pre_natal_period,
            "event_start_date is before person birth_datetime": 
            event_start_before_pre_natal_period,
            "event_start_date is after death_datetime (+42 days)": 
            event_start_after_death,
        }
        if "event_end_date" in table.get_column_names():
            no_event_end_date = "event_end_date is NULL"
            end_before_start = "event_end_date < event_start_date"
            event_end_before_birth = f"""
                EXISTS(
                    SELECT birth_datetime
                    FROM `{self.person_table_id}` AS person
                    WHERE src.person_id = person.person_id 
                        AND src.event_end_date < person.birth_datetime
                )
            """
            event_end_after_death = f"""
                EXISTS(
                    SELECT death_datetime
                    FROM `{self.person_table_id}` AS person
                    WHERE src.person_id = person.person_id 
                        AND person.death_datetime IS NOT NULL
                        AND src.event_end_date > DATETIME_ADD(person.death_datetime, 
                                                              INTERVAL 42 DAY)
                )
            """
            messages_with_problem_cases[
                "Entry has no event_end_date"
            ] = no_event_end_date
            messages_with_problem_cases[
                "event_end_date is before event_start_date"
            ] = end_before_start
            messages_with_problem_cases[
                "event_end_date is before person birth_datetime" 
            ] = event_end_before_birth
            messages_with_problem_cases[
                "event_end_date is after person death_datetime"
            ] = event_end_after_death


        problem_col_cases = ("CASE " + " ".join([
            f'WHEN {problem_sql} THEN "{problem_text}"'
            for problem_text, problem_sql in messages_with_problem_cases.items()
        ]) + ' ELSE "No problem" END')

        problem_tab_sql = f"""
            SELECT {problem_col_cases} AS problem, *
            FROM `{table.full_table_id}` AS src
            ORDER BY person_id
        """

        run_sql_query(problem_tab_sql, destination=table.full_table_id)
            
            
    def _split_problem_entries_from_src_tables(self):


        print("Separating out problem entries from source tables\n")
        for table in self.tables:

            print(f"    {table.table_id}:")
            self._add_problem_entries_column_to_table(table)
            problem_table_sql = f"""
                SELECT * FROM `{table.full_table_id}`
                WHERE problem != "No problem"
                ORDER BY person_id
            """
            problem_table_id = f"{table.full_table_id}_problems"
            problem_bq_table = run_sql_query(problem_table_sql, 
                                             destination=problem_table_id)
            print(f"\t* {problem_bq_table.num_rows} problem entries identified "
                  f"and removed to {table.table_id}_problems")

            src_table_sql = f"""
                SELECT * EXCEPT(problem) FROM `{table.full_table_id}`
                WHERE problem = "No problem"
                ORDER BY person_id
            """
            src_bq_table = run_sql_query(src_table_sql, 
                                         destination=table.full_table_id)
            print(f"\t* {src_bq_table.num_rows} entries remain in {table.table_id}")
            
        