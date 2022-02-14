import datetime
from FDM_helpers import *
from google.cloud import bigquery
import pandas as pd
import numpy as np 


# Set global variables
PROJECT = "yhcr-prd-phm-bia-core"
CLIENT = bigquery.Client(project=PROJECT)


def generate_random_dates(n=1, decade=1990):
    dates = np.concatenate(
        (np.random.choice(range(decade,decade+10), (n,1)), 
         np.random.choice(range(1,13), (n,1)), 
         np.random.choice(range(1,29), (n,1))), 
        axis=1
    )
    return pd.Series([datetime.date(year=year, month=month, day=day) 
                     for year, month, day in dates],
                     dtype="datetime64[ns]")
#     return pd.Series([f"{year}-{month}-{day}" 
#                       for year, month, day in dates],
#                      dtype="string")


def build_test_master_person_df():
    person_df = pd.DataFrame(dict(
        person_id = list(range(100)),
        birth_datetime = generate_random_dates(n=100),
        death_datetime = pd.Series([], dtype="datetime64[ns]")
    ))
    person_df.loc[person_df.person_id % 5 == 0, "death_datetime"] = (
        generate_random_dates(n=100, decade=2010)
    )
    return person_df


def build_test_demographics_df():
    
    demographics_df = pd.DataFrame([])

    demographics_df["person_id"] = [
        str(i) for i in range(100)
    ]
    demographics_df["digest"] = [
        chr(a) + chr(b) 
        for a in range(60, 70) 
        for b in range(70,80)
    ]
    demographics_df["EDRN"] = [
        chr(a) + chr(b) 
        for a in range(70, 80) 
        for b in range(80,90)
    ]
    return demographics_df


def add_junk_ids(df, n=5):
    junk_ints = np.random.randint(1000, size=(n,))
    data = {col: ["junk_" + col + f"_{i}" for i in junk_ints] 
            for col in df.columns}
    junk_df = pd.DataFrame(data=data, columns=df.columns)
    return (df.append(junk_df)
            .reset_index(drop=True))


def build_test_environment():
    
    master_dataset_id = f"{PROJECT}.CY_TESTS_MASTER"
    src_dataset_id = f"{PROJECT}.CY_TESTS_SRC"
    fdm_dataset_id = f"{PROJECT}.CY_TESTS_FDM"
    for dataset_id in [master_dataset_id, src_dataset_id, fdm_dataset_id]:
        try:
            CLIENT.get_dataset(dataset_id)
            clear_dataset(dataset_id)
        except:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "europe-west2"
            CLIENT.create_dataset(dataset, timeout=30)
    
    master_person_df = build_test_master_person_df()
    master_person_table_id = f"{master_dataset_id}.person"
    master_person_df.to_gbq(destination_table=master_person_table_id,
                            project_id=PROJECT,
                            table_schema=[{"name":"birth_datetime", "type": "DATE"},
                                          {"name":"death_datetime", "type": "DATE"}],
                            progress_bar=None)
    
    demographics_df = build_test_demographics_df()
    demographics_table_id = f"{master_dataset_id}.demographics"
    demographics_df.to_gbq(destination_table=demographics_table_id,
                            project_id=PROJECT,
                            progress_bar=None)
    
    src_table_1 = demographics_df.iloc[:20,:]
    src_table_1.drop(["digest", "EDRN"], axis=1, inplace=True)
    src_table_1 = add_junk_ids(src_table_1)
    src_table_1["date"] = pd.Series([], dtype="datetime64[ns]")
    src_table_1.iloc[:5,-1] = generate_random_dates(n=5, decade=1980)
    src_table_1.iloc[5:15,-1] = generate_random_dates(n=10, decade=2000)
    src_table_1.iloc[15:20,-1] = generate_random_dates(n=5, decade=2020)
    src_table_1.iloc[20:25,-1] = generate_random_dates(n=5, decade=2000)
    destination_table=f"{src_dataset_id}.src_table_1"
    src_table_1.to_gbq(destination_table=destination_table, 
                       progress_bar=None, 
                       if_exists="replace", 
                       table_schema=[{"name":"date", "type": "DATE"}], 
                       project_id=PROJECT)

    src_table_2 = demographics_df.iloc[10:30,:]
    src_table_2.drop(["person_id", "EDRN"], axis=1, inplace=True)
    src_table_2 = add_junk_ids(src_table_2)

    src_table_3 = demographics_df.iloc[20:40,:]
    src_table_3.drop(["person_id", "digest"], axis=1, inplace=True)
    src_table_3 = add_junk_ids(src_table_3)

    src_table_4 = demographics_df.iloc[30:50,:]
    src_table_4.drop(["EDRN"], axis=1, inplace=True)
    src_table_4 = add_junk_ids(src_table_4)

    src_table_5 = demographics_df.iloc[40:60,:]
    src_table_5.drop(["digest"], axis=1, inplace=True)
    src_table_5 = add_junk_ids(src_table_5)

    src_table_6 = demographics_df.iloc[50:70,:]
    src_table_6.drop(["person_id"], axis=1, inplace=True)
    src_table_6 = add_junk_ids(src_table_6)

    all_src_tables = [src_table_2, src_table_3, 
                      src_table_4, src_table_5, src_table_6]

    for idx, table in enumerate(all_src_tables):
        destination_table=f"{src_dataset_id}.src_table_{idx+2}"
        table.to_gbq(destination_table=destination_table,
                     progress_bar=None,
                     if_exists="replace",
                     project_id=PROJECT)
        
    