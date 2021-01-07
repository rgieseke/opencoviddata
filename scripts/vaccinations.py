from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz
from openpyxl import load_workbook

# https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Impfquoten-Tab.html

root = Path(__file__).parents[1]

excel_file = root / "raw/Impfquotenmonitoring.xlsx"


meta = [
    "Digitales Impfquotenmonitoring zur COVID-19-Impfung",
    "CSV-Version der Excel-Datei des RKI",
    "Quelle: https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Impfquoten-Tab.htm",
    f"Abfragezeitpunkt: { datetime.now(pytz.timezone('Europe/Berlin')).strftime('%Y-%m-%d %H:%M:%S') }",
    "",
]

wb = load_workbook(excel_file)
meta_sheet = wb[wb.sheetnames[0]]


def iter_rows(ws):
    for row in ws.iter_rows():
        yield [
            cell.value
            for cell in row
            if (not pd.isnull(cell.value)) and (not cell.value == "")
        ]


notes = list(iter_rows(meta_sheet))
for row in notes:
    if len(row) > 0:
        meta += [" ".join(row)]

df = pd.read_excel(excel_file, sheet_name=1, usecols="A:I", nrows=17, index_col=[0, 1])
df = df.drop("Impfungen pro 1.000 Einwohner", axis=1)
df = df.astype("Int64")
df = df.rename(columns={"Pflegeheim-bewohnerIn*": "PflegeheimbewohnerIn*"})

footnotes = wb[wb.sheetnames[1]]
for row in list(iter_rows(footnotes))[18:]:
    if len(row) > 0:
        meta += [" ".join(row)]

# Remove duplicates in metadata
meta = list(dict.fromkeys(meta))

with open(root / "data/vaccinations/vaccinations.csv", "w") as f:
    for line in meta:
        f.write(f"# {line}\n")
    f.write(df.to_csv())
