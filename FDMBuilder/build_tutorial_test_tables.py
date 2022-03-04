from FDMBuilder.testing_helpers import *

persons_sql = """
    SELECT person_id, birth_datetime, death_datetime
    FROM `CY_FDM_MASTER.person`
    LIMIT 50
"""
persons = CLIENT.query(persons_sql).to_dataframe() 

dead_persons_sql = """
    SELECT person_id, birth_datetime, death_datetime
    FROM `CY_FDM_MASTER.person`
    WHERE death_datetime IS NOT NULL
    LIMIT 50
"""
dead_persons = CLIENT.query(dead_persons_sql).to_dataframe() 

test_table_1 = persons.append(dead_persons).reset_index(drop=True)

test_table_1["start_date"] = test_table_1.birth_datetime

test_table_1.loc[40:44,"start_date"] = test_table_1.loc[40:44,"start_date"].apply(
    lambda x: sub_random_days(x, upper=294)
)

test_table_1.loc[45:49,"start_date"] = test_table_1.loc[45:49,"start_date"].apply(
    lambda x: sub_random_days(x, upper=3000)
)

test_table_1.loc[80:84,"start_date"] = test_table_1.loc[80:84,"death_datetime"].apply(
    lambda x: add_random_days(x, upper=3000)
)

test_table_1.loc[0:39,"start_date"] = (test_table_1
                                       .loc[0:39,"start_date"]
                                       .apply(add_random_days))

test_table_1.loc[50:79,"start_date"] = (test_table_1
                                       .loc[50:79,"start_date"]
                                       .apply(add_random_days))

test_table_1.loc[85:99,"start_date"] = (test_table_1
                                       .loc[85:99,"start_date"]
                                       .apply(add_random_days))

test_table_1.loc[0:5,"person_id"] = list(range(99999990, 99999996))

test_table_1["start_date"] = test_table_1.start_date.apply(
    lambda x: "-".join([str(x.day), str(x.month_name()), str(x.year)])
)

test_table_1.loc[30:34,"start_date"] = np.nan

test_table_1 = test_table_1[["person_id", "start_date"]]
test_table_1.to_gbq(destination_table="CY_FDM_BUILDER_TESTS.test_table_1",
                    project_id="yhcr-prd-phm-bia-core",
                    if_exists="replace")

persons_2_sql = """
    SELECT demo.digest, person.birth_datetime, person.death_datetime
    FROM `CY_FDM_MASTER.person` person
    INNER JOIN `CY_STAGING_DATABASE.src_DemoGraphics_MASTER` demo
    ON demo.person_id = person.person_id
    WHERE digest IS NOT NULL
    LIMIT 50
"""
persons_2 = CLIENT.query(persons_2_sql).to_dataframe()

dead_persons_2_sql = """
    SELECT demo.digest, person.birth_datetime, person.death_datetime
    FROM `CY_FDM_MASTER.person` person
    INNER JOIN `CY_STAGING_DATABASE.src_DemoGraphics_MASTER` demo
    ON demo.person_id = person.person_id
    WHERE digest IS NOT NULL
    AND death_datetime IS NOT NULL
    LIMIT 50
"""
dead_persons_2 = CLIENT.query(dead_persons_2_sql).to_dataframe()

test_table_2 = persons_2.append(dead_persons_2).reset_index(drop=True)

test_table_2.loc[10:14, "digest"] = [f"fake_digest_{i}" for i in range(1,6)]

test_table_2["start_date"] = test_table_2.birth_datetime.apply(add_random_days)

test_table_2.loc[40:44,"start_date"] = (test_table_2
                                        .loc[40:44,"birth_datetime"]
                                        .apply(
                                            lambda x: sub_random_days(x, upper=294)
                                        ))

test_table_2.loc[45:49,"start_date"] = (test_table_2
                                        .loc[45:49,"birth_datetime"]
                                        .apply(
                                            lambda x: sub_random_days(x, upper=3000)
                                        ))

test_table_2.loc[80:84,"start_date"] = (test_table_2
                                        .loc[80:84,"death_datetime"]
                                        .apply(
                                            lambda x: add_random_days(x, upper=3000)
                                        ))
test_table_2["start_month"] = test_table_2.start_date.apply(
    lambda x: x.month
)

test_table_2["start_year"] = test_table_2.start_date.apply(
    lambda x: x.year
)

test_table_2["end_date"] = test_table_2.start_date.apply(
    lambda x: add_random_days(x, upper=30)
)
test_table_2.loc[30:34, "end_date"] = (test_table_2
                                       .loc[30:34, "start_date"]
                                       .apply(
                                           lambda x: sub_random_days(x, 60)
                                       ))
test_table_2.loc[90:94, "end_date"] = (test_table_2
                                       .loc[90:94, "death_datetime"]
                                       .apply(add_random_days))
test_table_2.loc[20:24, "end_date"] = np.nan
test_table_2["end_month"] = test_table_2.end_date.apply(
    lambda x: x.month
)

test_table_2["end_year"] = test_table_2.end_date.apply(
    lambda x: x.year
)
test_table_2.rename({"digest":"wrong_digest"}, axis=1, inplace=True)
test_table_2 = test_table_2[["wrong_digest", "start_month", "start_year", 
                             "end_month", "end_year"]]
test_table_2.to_gbq(destination_table="CY_FDM_BUILDER_TESTS.test_table_2",
                    project_id="yhcr-prd-phm-bia-core",
                    if_exists="replace")