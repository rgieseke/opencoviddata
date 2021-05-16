import pandas as pd
import sqlite_utils

from pathlib import Path

from mappings import counties

root = Path(__file__).parents[1]

def create_table(db):
    db["survstat_counties"].create(
        {
            "id": str,
            "county": str,
            "state_id": str,
            "calendarweek": str,
            "agegroup": str,
            "cases": int,
            "incidence": float
        },
        pk=("id", "calendarweek", "agegroup")
    )
    db["survstat_counties"].create_index(["id"])
    db["survstat_counties"].create_index(["county"])
    db["survstat_counties"].create_index(["state_id"])
    db["survstat_counties"].create_index(["calendarweek"])
    db["survstat_counties"].create_index(["agegroup"])

db_name = "coronadata.db"

db = sqlite_utils.Database(db_name)

if 'survstat_counties' not in db.table_names():
    print("Creating table")
    create_table(db)


for county_name, county_values in counties.items():

    filename = f"data/counties/survstat-covid19-cases-{ county_name.lower().replace(' ', '-') }.csv"
    print(county_name, filename)
    with open(root / filename, "r") as f:
        df_cases = pd.read_csv(f, comment="#", index_col=0)

    current_week_day = pd.Timestamp.now().day_of_week
    # Keep past weeks from Tuesday once two days updates for the past
    # week are included.
    if current_week_day >= 2:
        df_cases = df_cases.iloc[:-1]
    else:  # Skip on-going week.
        df_cases = df_cases.iloc[:-2]

    df_cases_long = df_cases.reset_index().melt(id_vars="KW", var_name="agegroup", value_name="cases")
    df_cases_long = df_cases_long.rename(columns={"KW": "calendarweek"})
    df_cases_long["id"] = county_values["County"]
    df_cases_long["county"] = county_name
    df_cases_long["state_id"] = county_values["State"]

    db["survstat_counties"].upsert_all(
        df_cases_long.to_dict(orient="records"), pk=("id", "calendarweek", "agegroup"))


    filename = f"data/counties/survstat-covid19-incidence-{ county_name.lower().replace(' ', '-') }.csv"
    print(county_name, filename)

    with open(root / filename, "r") as f:
        df_incidence = pd.read_csv(f, comment="#", index_col=0)

    current_week_day = pd.Timestamp.now().day_of_week
    # Keep past weeks from Tuesday once two days updates for the past
    # week are included.
    if current_week_day >= 2:
        df_incidence = df_incidence.iloc[:-1]
    else:  # Skip on-going week.
        df_incidence = df_incidence.iloc[:-2]

    df_incidence_long = df_incidence.reset_index().melt(id_vars="KW", var_name="agegroup", value_name="incidence")
    df_incidence_long = df_incidence_long.rename(columns={"KW": "calendarweek"})
    df_incidence_long["id"] = county_values["County"]
    df_cases_long["county"] = county_name
    df_incidence_long["state_id"] = county_values["State"]

    db["survstat_counties"].upsert_all(
        df_incidence_long.to_dict(orient="records"), pk=("id", "calendarweek", "agegroup"))
