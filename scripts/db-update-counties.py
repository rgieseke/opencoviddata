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
            "incidence": float,
        },
        pk=("id", "calendarweek", "agegroup"),
    )
    db["survstat_counties"].create_index(["id"])
    db["survstat_counties"].create_index(["county"])
    db["survstat_counties"].create_index(["state_id"])
    db["survstat_counties"].create_index(["calendarweek"])
    db["survstat_counties"].create_index(["agegroup"])


db_name = "coronadata.db"

db = sqlite_utils.Database(db_name)

if "survstat_counties" in db.table_names():
    print("Recreating table")
    db["survstat_counties"].drop()

create_table(db)

df_counties = []

for county_name, county_values in counties.items():

    filename = f"data/counties/survstat-covid19-cases-{ county_name.lower().replace(' ', '-') }.csv"
    print(county_name, filename)
    with open(root / filename, "r") as f:
        df_cases = pd.read_csv(f, comment="#", index_col=0)

    df_cases_long = df_cases.reset_index().melt(
        id_vars="KW", var_name="agegroup", value_name="cases"
    )
    df_cases_long = df_cases_long.rename(columns={"KW": "calendarweek"})
    df_cases_long["id"] = county_values["County"]
    df_cases_long["county"] = county_name
    df_cases_long["state_id"] = county_values["State"]

    df_cases_long = df_cases_long.set_index(
        ["id", "county", "state_id", "calendarweek", "agegroup"]
    )

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

    df_incidence_long = df_incidence.reset_index().melt(
        id_vars="KW", var_name="agegroup", value_name="incidence"
    )
    df_incidence_long = df_incidence_long.rename(columns={"KW": "calendarweek"})
    df_incidence_long["id"] = county_values["County"]
    df_incidence_long["county"] = county_name
    df_incidence_long["state_id"] = county_values["State"]

    df_incidence_long = df_incidence_long.set_index(
        ["id", "county", "state_id", "calendarweek", "agegroup"]
    )

    df_counties.append(df_cases_long.join(df_incidence_long))

df_counties = pd.concat(df_counties).reset_index()

db["survstat_counties"].insert_all(
    df_counties.to_dict(orient="records"), pk=("id", "calendarweek", "agegroup")
)

# Calculate population shares of agegroups from cases and incidence numbers.
# Some testing showed no changes in data population between 2020/2021,
# using the maximum of cases per age group for calculation.
if "population_counties" in db.table_names():
    db["population_counties"].drop()

print("Creating population table")
db["population_counties"].create(
    {"id": str, "county": str, "agegroup": str, "population": int},
    pk=("id", "county", "agegroup"),
)
db["population_counties"].create_index(["id"])
db["population_counties"].create_index(["county"])
db["population_counties"].create_index(["agegroup"])

grouped = df_counties.groupby(["id", "county", "agegroup"]).max("cases")
grouped["population"] = grouped.apply(
    lambda x: x.cases / x.incidence * 100_000, axis=1
).round()

population = grouped.reset_index()[["id", "county", "agegroup", "population"]]
population = population.dropna()

db["population_counties"].insert_all(
    population.to_dict(orient="records"), pk=("id", "county", "agegroup")
)
