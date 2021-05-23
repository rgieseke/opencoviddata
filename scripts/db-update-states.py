import pandas as pd
import sqlite_utils

from pathlib import Path

from mappings import state_ids

root = Path(__file__).parents[1]

def create_table(db):
    db["survstat_states"].create(
        {
            "id": str,
            "state": str,
            "calendarweek": str,
            "agegroup": str,
            "cases": int,
            "incidence": float
        },
        pk=("id", "calendarweek", "agegroup")
    )
    db["survstat_states"].create_index(["id"])
    db["survstat_states"].create_index(["state"])
    db["survstat_states"].create_index(["calendarweek"])
    db["survstat_states"].create_index(["agegroup"])

db_name = "coronadata.db"

db = sqlite_utils.Database(db_name)

if 'survstat_states' in db.table_names():
    print("Recreating table")
    db['survstat_states'].drop()

create_table(db)

population = pd.read_csv(
        root / "data/12411-0012_flat.csv", encoding="ISO8859", delimiter=";"
    )

for state, idx in state_ids.items():

    pop_state = population[population["1_Auspraegung_Label"] == state]
    pop_state = pop_state[["2_Auspraegung_Code", "BEVSTD__Bevoelkerungsstand__Anzahl"]]
    pop_state.columns = ["Alter", "Anzahl"]

    pop_state = pop_state.dropna()  # Remove total row

    pop_state.index = [i[-2:] if not i[-2:] == "UM" else "90+" for i in pop_state.Alter]
    pop_state.index.name = "Alter"

    pop_state = pop_state.drop("Alter", axis=1)

    pop_state_grouped = pop_state.groupby(pop_state.reset_index().index // 5).sum()
    pop_state_grouped["Alter"] = [f"A{i:02d}..{i+4:02d}" for i in range(0, 80, 5)] + [
        "A80+",  # 80-84
        "A80+",  # 85-89
        "A80+",  # 90+
    ]
    pop_state_grouped = pop_state_grouped.groupby("Alter").sum()

    def calc_incidence(row):
      if row["agegroup"] == "Unbekannt":
          return None
      return (row["cases"] / pop_state_grouped.loc[row["agegroup"]].Anzahl * 100_000).round(2)


    filename = f"data/states/survstat-covid19-cases-{ state.lower()}.csv"
    print(idx, filename)
    with open(root / filename, "r") as f:
        df = pd.read_csv(f, comment="#", index_col=0)

    current_week_day = pd.Timestamp.now().day_of_week
    # Keep past weeks from Tuesday once two days updates for the past
    # week are included.
    if current_week_day >= 2:
        df = df.iloc[:-1]
    else:  # Skip on-going week.
        df = df.iloc[:-2]

    df_long = df.reset_index().melt(id_vars="KW", var_name="agegroup", value_name="cases")
    df_long = df_long.rename(columns={"KW": "calendarweek"})
    df_long["state"] = state
    df_long["id"] = idx
    df_long['incidence'] = df_long.apply(calc_incidence, axis=1)

    db["survstat_states"].upsert_all(
        df_long.to_dict(orient="records"), pk=("id", "calendarweek", "agegroup"))
