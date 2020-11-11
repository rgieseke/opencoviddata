from pathlib import Path

import altair as alt
import pandas as pd

from mappings import state_ids

root = Path(__file__).parents[1]


for state in state_ids.keys():

    population = pd.read_csv(
        root / "data/12411-0012_flat.csv", encoding="ISO8859", delimiter=";"
    )

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

    with open(root / f"data/survstat-covid19-cases-{ state.lower()}.csv", "r") as f:
        for line in f:
            if line.startswith("# Abfragezeitpunkt"):
                data_date = line.split(":", maxsplit=1)[1].strip()
                data_date = pd.Timestamp(data_date)
                break
    print(state, data_date)

    cases = pd.read_csv(
        root / f"data/survstat-covid19-cases-{ state.lower()}.csv",
        index_col=0,
        comment="#",
    )
    cases = cases.astype("Int64")

    # Only use complete weeks.
    if data_date.isocalendar()[1] == int(cases.index[-1].split("KW")[1]):
        cases = cases.drop(cases.index[-1])

    long_data = cases.reset_index().melt(
        id_vars="KW", var_name="Alter", value_name="Fälle"
    )

    pop_state_grouped["Share"] = (
        pop_state_grouped.Anzahl / pop_state_grouped.Anzahl.sum()
    )
    pop_state_grouped["ShareLower"] = pop_state_grouped.Share.cumsum().shift(
        fill_value=0
    )
    pop_state_grouped["ShareUpper"] = pop_state_grouped.Share.cumsum()

    long_data = pop_state_grouped.join(long_data.set_index("Alter")).reset_index()

    ages = long_data.Alter.unique()
    values = long_data.ShareLower.unique()
    label_expr = ""
    for i in range(17):
        label_expr += f"datum.value == {values[i]} ? '{ages[i]}' : "
    label_expr += "null"

    base = (
        alt.Chart(long_data)
        .transform_calculate(Inzidenz="datum['Fälle'] / datum['Anzahl'] * 100000")
        .properties(
            height=510,
            width=907,
            title=[
                f"7-Tages-Inzidenz von COVID-19 je Kalenderwoche nach Altersgruppen in {state}",
                f"Datenquelle: Robert Koch-Institut, SurvStat@RKI 2.0, https://survstat.rki.de, Abfragedatum: {data_date.strftime('%d.%m.%Y')}",
            ],
        )
        .encode(
            alt.X("KW:O", axis=alt.Axis(title="Kalenderwoche", labelAngle=-45)),
            alt.Y(
                "ShareLower:Q",
                axis=alt.Axis(
                    title="Altersgruppe",
                    tickCount=17,
                    values=values,
                    labelExpr=label_expr,
                    labelBaseline=alt.TextBaseline("line-bottom"),
                ),
            ),
        )
    )

    tooltip = [
        "KW",
        "Alter",
        alt.Tooltip("Anzahl", format=","),
        "Fälle",
        alt.Tooltip("Inzidenz:Q", format=".1f"),
    ]
    chart = base.mark_rect().encode(
        y2="ShareUpper",
        color=alt.Color(
            "Inzidenz:Q",
            scale=alt.Scale(scheme="inferno"),
            legend=alt.Legend(direction="vertical"),
        ),
        tooltip=tooltip,
    )

    # Configure text
    text = base.mark_text(baseline="bottom", fontSize=9).encode(
        text=alt.Text("Inzidenz:Q", format="d"),
        color=alt.condition(
            alt.datum.Inzidenz >= 99.5, alt.value("black"), alt.value("gray")
        ),
        tooltip=tooltip,
    )

    (chart + text).save(str(root / f"www/{ state.lower() }.html"))
