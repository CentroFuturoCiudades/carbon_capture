import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import pandas as pd
import seaborn as sns
from matplotlib.figure import Figure

import dagster as dg
from afolu.defs.assets.constants import COLUMN_COLOR_MAP, COLUMN_NAME_MAP
from afolu.defs.partitions import wanted_zones_partitions


@dg.asset(
    name="area",
    key_prefix=["small", "plot"],
    ins={"df_area": dg.AssetIn(["small", "area", "table_merged"])},
    partitions_def=wanted_zones_partitions,
    io_manager_key="figure_manager",
    group_name="small_plot",
)
def area_plot(df_area: pd.DataFrame) -> Figure:
    sns.set_theme(style="ticks", font_scale=2)

    df_area = (
        df_area.set_index("label")
        .T.reset_index(names="year")
        .assign(year=lambda df: df["year"].astype(int) + 2000)
        .query("2000 <= year <= 2020")
        .assign(year=lambda df: df["year"].astype(str))
        .set_index("year")
        .divide(100)
        .rename(columns=COLUMN_NAME_MAP)
    )

    masked = (df_area.div(df_area.sum(axis=1), axis=0) < 0.01).all(axis=0)
    masked_cols = masked[~masked].index

    fig, ax = plt.subplots(figsize=(14, 6))
    df_area[masked_cols].plot.area(ax=ax, color=COLUMN_COLOR_MAP, alpha=0.75, lw=0)
    ax.legend(bbox_to_anchor=(1, 0.5))

    ax.set_xlim(0, 20)
    ax.set_ylim(0, df_area.sum(axis=1).max() * 0.97)

    ax.set_xlabel("")
    ax.set_ylabel("Área (km²)")

    ax.yaxis.set_major_formatter(mtick.StrMethodFormatter("{x:,.0f}"))

    ax.set_title("Área por clase de uso de suelo")
    return fig
