import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.figure import Figure

import dagster as dg
from afolu.partitions import wanted_zones_partitions


@dg.asset(
    name="emissions",
    key_prefix=["small", "plot"],
    ins={"df_emissions": dg.AssetIn(["small", "emissions"])},
    partitions_def=wanted_zones_partitions,
    io_manager_key="figure_manager",
    group_name="small_plot",
)
def emissions_plot(df_emissions: pd.DataFrame) -> Figure:
    sns.set_theme(style="ticks", font_scale=1.8)

    name_map = {
        "emission_co2e_subsector_total_frst": "Bosques",
        "emission_co2e_subsector_total_lndu": "Transiciones de uso de suelo",
        "emission_co2e_co2_soil_soc_mineral_soils": "Mineralización del suelo",
        "emission_co2e_n2o_soil_mineral_soils": "Mineralización del suelo\n(equivalente N2O)",
        "emission_co2e_n2o_soil_organic_soils": "Suelo orgánico\n(equivalente N2O)",
    }

    df_emissions = (
        df_emissions.query("time_period < 21")
        .filter([*list(name_map.keys()), "time_period"], axis="columns")
        .rename(columns=name_map)
        .assign(time_period=lambda df: (df["time_period"] + 2000))
        .set_index("time_period")
    )

    fig, ax = plt.subplots(figsize=(14, 6))
    df_emissions.plot.bar(stacked=True, ax=ax, width=0.9, legend=False)

    (
        df_emissions.sum(axis=1)
        .reset_index()
        .assign(time_period=lambda df: df["time_period"].astype(str))
        .set_index("time_period")[0]
        .plot(ax=ax, c="k", label="Neto", lw=2, legend=False)
    )

    ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
    ax.axhline(0, 0, 1, c="k", ls="--")

    ax.set_xlabel("")
    ax.set_ylabel("CO2 emitido (millones de toneladas)")

    ax.legend(loc="upper left", bbox_to_anchor=(1, 1))

    fig.tight_layout()
    return fig
