import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.figure import Figure
from mpl_chord_diagram import chord_diagram

import dagster as dg
from afolu.defs.assets.constants import COLUMN_COLOR_MAP, COLUMN_NAME_MAP
from afolu.defs.partitions import wanted_zones_partitions


@dg.asset(
    name="transitions",
    key_prefix=["small", "plot"],
    ins={
        "df_table_fixed_extra": dg.AssetIn(
            ["small", "transition", "table_fixed_extra"],
        ),
    },
    partitions_def=wanted_zones_partitions,
    io_manager_key="figure_manager",
    group_name="small_plot",
)
def transitions_plot(df_table_fixed_extra: pd.DataFrame) -> Figure:
    pivot = df_table_fixed_extra.set_index("start").sort_index().sort_index(axis=1)

    mask = (pivot.sum() > 10 * 1e6) & (pivot.sum(axis=1) > 10 * 1e6)
    wanted_idx = mask[mask].index

    pivot = pivot.filter(wanted_idx, axis=0).filter(wanted_idx, axis=1)

    names = [COLUMN_NAME_MAP[c] for c in pivot.index]

    mat = pivot.to_numpy()
    np.fill_diagonal(mat, 0)

    fig, ax = plt.subplots(figsize=(8, 8))
    chord_diagram(
        mat,
        ax=ax,
        names=names,
        directed=True,
        # rotate_names=rotate_names,
        colors=[COLUMN_COLOR_MAP[c] for c in names],
        fontsize=16,
    )
    return fig
