from FDMBuilder.testing_helpers import *

# Collects random rows from master person table and builds "dummy" source tables 
# for use in tutorial scripts and testing for FDMBuilder

# collect 50 random people from person table
persons_sql = """
    SELECT person_id, birth_datetime, death_datetime
    FROM `CB_FDM_MASTER.person`
    LIMIT 50
"""
persons = CLIENT.query(persons_sql).to_dataframe() 

# collect another 50 random people with a death_datetime from person table
dead_persons_sql = """
    SELECT person_id, birth_datetime, death_datetime
    FROM `CB_FDM_MASTER.person`
    WHERE death_datetime IS NOT NULL
    LIMIT 50
"""
dead_persons = CLIENT.query(dead_persons_sql).to_dataframe() 

# stitch together the two sets of random people to form a 100 person dataframe
test_table_1 = persons.append(dead_persons).reset_index(drop=True)

# generate random 'start_date' from birth_datetime/death_datetime - deliberately 
# introduce some start dates that are before birth_datetime/after death_datetime
# and some NULLs
test_table_1["start_date"] = test_table_1.birth_datetime.apply(add_random_days)
test_table_1.loc[35:39,"start_date"] = np.nan
test_table_1.loc[40:44,"start_date"] = (
    test_table_1.loc[40:44,"birth_datetime"]
    .apply(lambda x: sub_random_days(x, upper=294))
)
test_table_1.loc[45:49,"start_date"] = (
    test_table_1.loc[45:49,"birth_datetime"]
    .apply(lambda x: sub_random_days(x, upper=3000))
)
test_table_1.loc[80:84,"start_date"] = (
    test_table_1.loc[80:84,"death_datetime"]
    .apply(lambda x: add_random_days(x, upper=3000))
)
# create some nonesense person_id entries
test_table_1.loc[0:5,"person_id"] = list(range(99999990, 99999996))
# reformat start_date as string e.g. "15-Jan-2002"
test_table_1["start_date"] = test_table_1.start_date.apply(
    lambda x: "-".join([str(x.day), str(x.month_name()), str(x.year)])
)
# select only person_id and start_date columns
test_table_1 = test_table_1[["person_id", "start_date"]]
# add some random data
test_table_1["some_data"] = np.random.choice(range(100000), 100)
# upload table to gbq
test_table_1.to_gbq(destination_table="CB_FDM_BUILDER_TESTS.test_table_1",
                    project_id="yhcr-prd-phm-bia-core",
                    if_exists="replace")

# select random entries from master person table that have a corresponding digest
persons_2_sql = """
    SELECT demo.digest, person.birth_datetime, person.death_datetime
    FROM `CB_FDM_MASTER.person` person
    INNER JOIN `CB_STAGING_DATABASE.src_DemoGraphics_MASTER` demo
    ON demo.person_id = person.person_id
    WHERE digest IS NOT NULL
    LIMIT 50
"""
persons_2 = CLIENT.query(persons_2_sql).to_dataframe()
# select random entries from master person table with a death_datetime and a 
# corresponding digest
dead_persons_2_sql = """
    SELECT demo.digest, person.birth_datetime, person.death_datetime
    FROM `CB_FDM_MASTER.person` person
    INNER JOIN `CB_STAGING_DATABASE.src_DemoGraphics_MASTER` demo
    ON demo.person_id = person.person_id
    WHERE digest IS NOT NULL
    AND death_datetime IS NOT NULL
    LIMIT 50
"""
dead_persons_2 = CLIENT.query(dead_persons_2_sql).to_dataframe()
# stich two dataframes together
test_table_2 = persons_2.append(dead_persons_2).reset_index(drop=True)
# create some nonesense digests for testing
test_table_2.loc[10:14, "digest"] = [f"fake_digest_{i}" for i in range(1,6)]

# generate random 'start_date' from birth_datetime/death_datetime - deliberately 
# introduce some start dates that are before birth_datetime/after death_datetime
# and some NULLs
test_table_2["start_date"] = test_table_2.birth_datetime.apply(add_random_days)
test_table_2.loc[40:44,"start_date"] = (
    test_table_2.loc[40:44,"birth_datetime"]
    .apply(lambda x: sub_random_days(x, upper=294))
)
test_table_2.loc[45:49,"start_date"] = (
    test_table_2.loc[45:49,"birth_datetime"]
    .apply(lambda x: sub_random_days(x, upper=3000))
)
test_table_2.loc[80:84,"start_date"] = (
    test_table_2.loc[80:84,"death_datetime"]
    .apply(lambda x: add_random_days(x, upper=3000))
)
# create individual start_month and start_year columns
test_table_2["start_month"] = test_table_2.start_date.apply(
    lambda x: x.month
)
test_table_2["start_year"] = test_table_2.start_date.apply(
    lambda x: x.year
)
# generate end_month, end_year in same way as start
test_table_2["end_date"] = test_table_2.start_date.apply(
    lambda x: add_random_days(x, upper=30)
)
test_table_2.loc[30:34, "end_date"] = (
    test_table_2.loc[30:34, "start_date"]
    .apply(lambda x: sub_random_days(x, 60))
)
test_table_2.loc[90:94, "end_date"] = (
    test_table_2.loc[90:94, "death_datetime"]
    .apply(add_random_days)
)
test_table_2.loc[20:24, "end_date"] = np.nan
test_table_2["end_month"] = test_table_2.end_date.apply(
    lambda x: x.month
)

test_table_2["end_year"] = test_table_2.end_date.apply(
    lambda x: x.year
)
# deliberately mis-name digest column for testing
test_table_2.rename({"digest":"wrong_digest"}, axis=1, inplace=True)
# select desired columns - drop those not required
test_table_2 = test_table_2[["wrong_digest", "start_month", "start_year", 
                             "end_month", "end_year"]]
# create random data column
test_table_2["some_data"] = np.random.choice(range(100000), 100)
test_table_2.to_gbq(destination_table="CB_FDM_BUILDER_TESTS.test_table_2",
                    project_id="yhcr-prd-phm-bia-core",
                    if_exists="replace")

# very similar process applies for creating third test table - see above comments
# for guidance
persons_3_sql = """
    SELECT demo.EDRN, person.birth_datetime, person.death_datetime
    FROM `CB_FDM_MASTER.person` person
    INNER JOIN `CB_STAGING_DATABASE.src_DemoGraphics_MASTER` demo
    ON demo.person_id = person.person_id
    WHERE EDRN IS NOT NULL
    LIMIT 50
"""
persons_3 = CLIENT.query(persons_3_sql).to_dataframe()

dead_persons_3_sql = """
    SELECT demo.EDRN, person.birth_datetime, person.death_datetime
    FROM `CB_FDM_MASTER.person` person
    INNER JOIN `CB_STAGING_DATABASE.src_DemoGraphics_MASTER` demo
    ON demo.person_id = person.person_id
    WHERE EDRN IS NOT NULL
    AND death_datetime IS NOT NULL
    LIMIT 50
"""
dead_persons_3 = CLIENT.query(dead_persons_3_sql).to_dataframe()

test_table_3 = persons_3.append(dead_persons_3).reset_index(drop=True)

test_table_3.loc[10:14, "digest"] = [f"fake_EDRN_{i}" for i in range(1,6)]

test_table_3["start_date"] = test_table_3.birth_datetime.apply(add_random_days)

test_table_3.loc[40:59,"start_date"] = (
    test_table_3.loc[40:59,"birth_datetime"]
    .apply(lambda x: sub_random_days(x, upper=300))
)
test_table_3.loc[65:69,"start_date"] = (
    test_table_3.loc[65:69,"birth_datetime"]
    .apply(lambda x: sub_random_days(x, upper=3000))
)
test_table_3.loc[80:84,"start_date"] = (
    test_table_3.loc[80:84,"death_datetime"]
    .apply(lambda x: add_random_days(x, upper=3000))
)
test_table_3["end_date"] = test_table_3.start_date.apply(
    lambda x: add_random_days(x, upper=20)
)
test_table_3.loc[30:34, "end_date"] = (
    test_table_3.loc[30:34, "start_date"]
    .apply(lambda x: sub_random_days(x, 60))
)
test_table_3.loc[90:94, "end_date"] = (
    test_table_3.loc[90:94, "death_datetime"].apply(add_random_days)
)
# creating examination_period string from start/end dates for testing
# e.g. "Nov/2013-Jan/2017"
test_table_3["examination_period"] = test_table_3.apply(
    lambda x: (x.start_date.month_name()[:3] + "/" + str(x.start_date.year)
               + "-"
               + x.end_date.month_name()[:3] + "/" + str(x.end_date.year)),
    axis=1
)
test_table_3["some_data"] = np.random.choice(range(100000), 100)
test_table_3.rename({"EDRN":"education_reference"}, axis=1, inplace=True)
test_table_3.drop(["birth_datetime", "death_datetime", "digest", 
                   "start_date", "end_date"], 
                  axis=1, 
                  inplace=True)
test_table_3.to_gbq(destination_table="CB_FDM_BUILDER_TESTS.test_table_3",
                    project_id="yhcr-prd-phm-bia-core",
                    if_exists="replace")