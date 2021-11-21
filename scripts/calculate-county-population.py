from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz
import time

from zeep import Client

from mappings import counties


root = Path(__file__).parents[1]

client = Client("https://tools.rki.de/SurvStat/SurvStatWebService.svc?wsdl")

factory = client.type_factory("ns2")


def fetch_county_cases():

    res = client.service.GetOlapData(
        {
            "Language": "German",
            "Measures": {"Count": 0},
            "Cube": "SurvStat",
            # Totals still included, setting `true` yields duplicates
            "IncludeTotalColumn": False,
            "IncludeTotalRow": False,
            "IncludeNullRows": False,
            "IncludeNullColumns": True,
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
                            "DimensionId": "[ReportingDate]",
                            "HierarchyId": "[ReportingDate].[Year]",
                        },
                        "Value": factory.FilterMemberCollection(
                            [
                                "[ReportingDate].[WeekYear].&[2021]",
                                "[ReportingDate].[WeekYear].&[2020]",
                            ]
                        ),
                    },
                ]
            ),
            "RowHierarchy": "[DeutschlandNodes].[Kreise71Web].[CountyKey71]",
            "ColumnHierarchy": "[AlterPerson80].[AgeGroupName8]",
        }
    )

    columns = [i["Caption"] for i in res.Columns.QueryResultColumn]

    df = pd.DataFrame(
        [
            [i["Caption"]]
            + [int(c) if c is not None else None for c in i["Values"]["string"]]
            for i in res.QueryResults.QueryResultRow
        ],
        columns=["County"] + columns,
    )
    df = df.set_index("County")
    df = df.astype("Int64")  # Allow for None values

    # df = df.drop("Gesamt", axis=1)
    df = df.drop("Unbekannt", axis=1)

    return df


def fetch_county_total_incidence():

    res = client.service.GetOlapData(
        {
            "Language": "German",
            "Measures": {"Incidence": 1},
            "Cube": "SurvStat",
            # Totals still included, setting `true` yields duplicates
            "IncludeTotalColumn": False,
            "IncludeTotalRow": False,
            "IncludeNullRows": False,
            "IncludeNullColumns": True,
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
                            "DimensionId": "[ReportingDate]",
                            "HierarchyId": "[ReportingDate].[Year]",
                        },
                        "Value": factory.FilterMemberCollection(
                            [
                                "[ReportingDate].[WeekYear].&[2021]",
                                "[ReportingDate].[WeekYear].&[2020]",
                            ]
                        ),
                    },
                ]
            ),
            "RowHierarchy": "[DeutschlandNodes].[Kreise71Web].[CountyKey71]",
            "ColumnHierarchy": "[AlterPerson80].[AgeGroupName8]",
        }
    )

    columns = [i["Caption"] for i in res.Columns.QueryResultColumn]

    df = pd.DataFrame(
        [
            [i["Caption"]]
            + [
                # Convert to point as decimal separator
                float(c.replace(".", "").replace(",", ".")) if c is not None else None
                for c in i["Values"]["string"]
            ]
            for i in res.QueryResults.QueryResultRow
        ],
        columns=["County"] + columns,
    )
    df = df.set_index("County")

    # df = df.drop("Gesamt", axis=1)
    df = df.drop("Unbekannt", axis=1)

    return df


if __name__ == "__main__":
    df_cases = fetch_county_cases()
    df_rate = fetch_county_total_incidence()

    df_pop = round(df_cases / df_rate * 100_000).astype("Int64")

    header = f"""# Quelle: »Robert Koch-Institut: SurvStat@RKI 2.0, https://survstat.rki.de«
# Abfragezeitpunkt: { datetime.now(pytz.timezone('Europe/Berlin')).strftime("%Y-%m-%d") }
# Berechnung der Einwohnerzahl je Altersgruppe aus API-Download der Covid19-Fälle je 100.000 Einwohner
"""
    with open(
        root / f"data/survstat-covid19-counties-population.csv",
        "w",
    ) as f:
        f.write(header)
        f.write(df_pop.to_csv())

    print(df_pop)
