import yaml
from pathlib import Path

root = Path(__file__).parents[1]

filename = "data/states/survstat-covid19-cases-berlin.csv"

with open(root / filename, "r") as f:
    f.readline()
    date = f.readline().split("Abfragezeitpunkt: ")[1].split(" ")[0]

print(date)
data_source = f"Robert Koch-Institut: SurvStat@RKI 2.0, https://survstat.rki.de, Abfragedatum: {date}"
print(data_source)

metadata = yaml.safe_load(open(root / "metadata.yaml", "r"))

metadata["source"] = data_source

metadata = yaml.dump(
    metadata, open(root / "metadata.yaml", "w"), sort_keys=False, allow_unicode=True
)
