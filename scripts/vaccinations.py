from pathlib import Path

import pandas as pd

# https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Impfquoten-Tab.html

root = Path(__file__).parents[1]

excel_file = root / "raw/Impfquotenmonitoring.xlsx"


meta = ["Digitales Impfquotenmonitoring zur COVID-19-Impfung",
"Quelle: https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Impfquoten-Tab.htm", ""]

notes = pd.read_excel(excel_file, sheet_name=0, index_col=0)
meta += [str(i) for i in notes.dropna(how="all", axis=1).index.values if not pd.isnull(i)]

df = pd.read_excel(excel_file, sheet_name=1, usecols="A:G", nrows=17, index_col=0)
df = df.astype("Int64")
df = df.rename(columns={"Pflegeheim-bewohnerIn": "PflegeheimbewohnerIn"})

footnotes = pd.read_excel(excel_file, sheet_name=1, skiprows=18, index_col=0)

meta += [
        str(i)
        for i in footnotes.dropna(how="all", axis=1).index.values
        if not pd.isnull(i)
    ]

# Remove duplicates in metadata
meta = list(dict.fromkeys(meta))

with open(root / "data/vaccinations/vaccinations.csv", "w") as f:
    for line in meta:
        f.write(f"# {line}\n")
    f.write(df.to_csv())
