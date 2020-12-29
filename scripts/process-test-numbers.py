import pandas as pd
from pathlib import Path


root = Path(__file__).parents[1]

excel_file = root / "raw/Testzahlen-gesamt.xlsx"

test_numbers = pd.read_excel(
    excel_file, sheet_name="Testzahlen", skiprows=2, skipfooter=2, usecols="B:F",
)
test_numbers["Positiven-quote (%)"] = test_numbers["Positiven-quote (%)"].round(2)


def rewrite_index(calendar_week):
    if type(calendar_week) == int:
        return "2020-KW" + str(calendar_week)
    elif calendar_week.endswith("*"):
        return "2020-KW" + calendar_week[:2]
    elif calendar_week.endswith("KW10"):
        return None
    else:
        raise Exception


test_numbers["KW"] = test_numbers["Kalenderwoche 2020"].apply(rewrite_index)
test_numbers = test_numbers.set_index("KW")
test_numbers = test_numbers.rename(
    columns={"Anzahl übermittelnde Labore": "Anzahl übermittelnde Labore (Tests)"}
)

test_capacity = pd.read_excel(
    excel_file,
    sheet_name="Testkapazitäten",
    skiprows=1,
    usecols="A:E",
    na_values="-",
    index_col=0,
)
# A few values include decimal places, round to precision shown in Excel
test_capacity = test_capacity.round().astype("Int64")
test_capacity = test_capacity.reset_index()
test_capacity["KW"] = test_capacity[
    "KW, für die die Angabe prognostisch erfolgt ist:"
].apply(lambda x: "2020-" + x)
test_capacity = test_capacity.set_index("KW")
test_capacity = test_capacity.drop(
    "KW, für die die Angabe prognostisch erfolgt ist:", axis=1
)
test_capacity = test_capacity.rename(
    columns={"Anzahl übermittelnde Labore": "Anzahl übermittelnde Labore (Kapazität)"}
)

backlog = pd.read_excel(excel_file, sheet_name="Probenrückstau", usecols="A:C",)
backlog["KW"] = backlog["KW"].apply(lambda x: "2020-KW" + str(x))
backlog = backlog.set_index("KW")
backlog = backlog.round().astype("Int64")

df = test_numbers.join(test_capacity).join(backlog)

header = f"""# Quelle: Robert Koch-Institut
# CSV-Version der auf
# https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Testzahl.html
# verfügbaren Excel-Datei.
#
# - Nachmeldungen für vergangene Kalenderwochen möglich
# - Testanzahl ist ungleich Anzahl Getesteter, es können Mehrfachtestungen enthalten sein
# - ab Kalenderwoche 46 (3. November 2020) geänderte Testkriterien
"""
with open(root / "data/test-numbers.csv", "w") as f:
    f.write(header)
    f.write(df.to_csv())

print(df.tail())
