from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz
import time

from zeep import Client

from mappings import state_ids

root = Path(__file__).parents[1]

client = Client("https://tools.rki.de/SurvStat/SurvStatWebService.svc?wsdl")

factory = client.type_factory("ns2")


def fetch_state(state):
    res = client.service.GetOlapData(
        {
            "Language": "German",
            "Measures": {"Count": 0},
            "Cube": "SurvStat",
            # Totals still included, setting `true` yields duplicates
            "IncludeTotalColumn": False,
            "IncludeTotalRow": False,
            "IncludeNullRows": False,
            "IncludeNullColumns": False,
            "HierarchyFilters": factory.FilterCollection(
                [
                    {
                        "Key": {
                            "DimensionId": "[PathogenOut].[KategorieNz]",
                            "HierarchyId": "[PathogenOut].[KategorieNz].[Krankheit DE]",
                        },
                        "Value": factory.FilterMemberCollection(
                            ["[PathogenOut].[KategorieNz].[Krankheit DE].&[COVID-19]"]
                        ),
                    },
                    {
                        "Key": {
                            "DimensionId": "[ReferenzDefinition]",
                            "HierarchyId": "[ReferenzDefinition].[ID]",
                        },
                        "Value": factory.FilterMemberCollection(
                            ["[ReferenzDefinition].[ID].&[1]"]
                        ),
                    },
                    {
                        "Key": {
                            "DimensionId": "[DeutschlandNodes].[Kreise71Web]",
                            "HierarchyId": "[DeutschlandNodes].[Kreise71Web].[FedStateKey71]",
                        },
                        "Value": factory.FilterMemberCollection(
                            [
                                f"[DeutschlandNodes].[Kreise71Web].[FedStateKey71].&[{ state_ids[state] }]"
                            ]
                        ),
                    },
                ]
            ),
            "RowHierarchy": "[ReportingDate].[YearWeek]",
            "ColumnHierarchy": "[AlterPerson80].[AgeGroupName6]",
        }
    )
    columns = [i["Caption"] for i in res.Columns.QueryResultColumn]

    df = pd.DataFrame(
        [
            [i["Caption"]]
            + [int(c) if c is not None else None for c in i["Values"]["string"]]
            for i in res.QueryResults.QueryResultRow
        ],
        columns=["KW"] + columns,
    )
    df = df.set_index("KW")
    df = df.astype("Int64")  # Allow for None values
    df = df.drop("Gesamt")
    df = df.drop("Gesamt", axis=1)

    header = f"""# Quelle: Robert Koch-Institut, SurvStat@RKI 2.0, https://survstat.rki.de
# Abfragezeitpunkt: { datetime.now(pytz.timezone('Europe/Berlin')).strftime("%Y-%m-%d %H:%M:%S") }
# API-Download für Covid19-Fälle in {state}, letzte KW ggf. noch nicht vollständig
"""
    with open(root / f"data/survstat-covid19-cases-{ state.lower()}.csv", "w") as f:
        f.write(header)
        f.write(df.to_csv())
    time.sleep(1)

if __name__ == "__main__":
    for state in state_ids.keys():
        print(state)
        fetch_state(state)
