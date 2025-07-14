from collections.abc import Sequence

import ee
import numpy as np
import pandas as pd

import dagster as dg
from afolu.assets.constants import LABEL_LIST, REDUCE_SCALE
from afolu.partitions import year_pair_partitions


def year_to_band_name(year: int | str) -> str:
    if isinstance(year, str):
        year = int(year)
    return f"b{year - 1999}"


def get_raster_area(raster: ee.image.Image, bbox: ee.geometry.Geometry) -> float:
    band_names = raster.bandNames().getInfo()

    if not isinstance(band_names, Sequence):
        err = "Band names should be a sequence."
        raise TypeError(err)

    band_count = len(band_names)

    if band_count != 1:
        err = f"Expected 1 band, got {band_count} bands: {band_names}"
        raise ValueError(err)

    response = (
        raster.rename(["band"])
        .multiply(ee.image.Image.pixelArea())
        .reduceRegion(
            reducer=ee.reducer.Reducer.sum(),
            scale=REDUCE_SCALE,
            geometry=bbox,
            maxPixels=int(1e10),
        )
        .getInfo()
    )

    if response is None:
        err = "No data returned from reduceRegion."
        raise ValueError(err)

    return float(
        response["band"],
    )


def transition_table_fixed_factory(
    top_prefix: str,
    partitions_def: dg.PartitionsDefinition | None = None,
    name_suffix: str = "",
) -> dg.AssetsDefinition:
    if partitions_def is None:
        partitions_def = year_pair_partitions

    @dg.asset(
        name=f"table_fixed{name_suffix}",
        key_prefix=[top_prefix, "transition"],
        ins={"table": dg.AssetIn([top_prefix, "transition", f"table{name_suffix}"])},
        partitions_def=partitions_def,
        io_manager_key="dataframe_manager",
        group_name=f"{top_prefix}_transition",
    )
    def _asset(table: pd.DataFrame) -> pd.DataFrame:
        table = table.set_index("start")

        for start in LABEL_LIST:
            if start == "forests_primary":
                continue

            table.loc[start, "forests_secondary"] = np.nansum(
                [
                    table.loc[start, "forests_secondary"],
                    table.loc[start, "forests_primary"],
                ],
            )
            table.loc[start, "forests_primary"] = np.nan

        return table.fillna(0)

    return _asset


def transition_table_frac_factory(
    top_prefix: str,
    partitions_def: dg.PartitionsDefinition | None = None,
    name_suffix: str = "",
) -> dg.AssetsDefinition:
    if partitions_def is None:
        partitions_def = year_pair_partitions

    @dg.asset(
        name=f"table_frac{name_suffix}",
        key_prefix=[top_prefix, "transition"],
        ins={
            "cross_fixed": dg.AssetIn(
                [top_prefix, "transition", f"table_fixed{name_suffix}"],
            ),
        },
        partitions_def=partitions_def,
        io_manager_key="dataframe_manager",
        group_name=f"{top_prefix}_transition",
    )
    def _asset(cross_fixed: pd.DataFrame) -> pd.DataFrame:
        cross_fixed = cross_fixed.set_index("start")

        zero_rows = cross_fixed.index[cross_fixed.sum(axis=1) == 0]
        for elem in zero_rows:
            cross_fixed.loc[elem, elem] = 1

        return cross_fixed.divide(cross_fixed.sum(axis=1), axis=0)

    return _asset


def transition_cube_factory(
    top_prefix: str,
    partitions_def: dg.PartitionsDefinition | None = None,
    name_suffix: str = "",
) -> dg.AssetsDefinition:
    @dg.asset(
        name=f"cube{name_suffix}",
        key_prefix=[top_prefix, "transition"],
        ins={
            "table_frac_map": dg.AssetIn(
                [top_prefix, "transition", f"table_frac{name_suffix}"],
            ),
        },
        partitions_def=partitions_def,
        io_manager_key="dataframe_manager",
        group_name=f"{top_prefix}_transition",
    )
    def _asset(table_frac_map: dict[str, pd.DataFrame]) -> pd.DataFrame:
        table_frac_map = {key.split("|")[0]: df for key, df in table_frac_map.items()}

        time_periods = [
            f"{start_year}_{end_year}"
            for start_year, end_year in zip(
                range(2000, 2022),
                range(2001, 2023),
                strict=True,
            )
        ]
        time_period_map = {key: i for i, key in enumerate(time_periods)}

        rows = []
        for period in time_periods:
            table = table_frac_map[period].set_index("start")
            for start_label in sorted(LABEL_LIST):
                for end_label in sorted(LABEL_LIST):
                    rows.append(  # noqa: PERF401
                        {
                            "transition": f"pij_lndu_{start_label}_to_{end_label}",
                            "time_period": time_period_map[period],
                            "value": table.loc[start_label, end_label],
                        },
                    )

        return pd.DataFrame(rows).pivot_table(
            index="time_period",
            columns="transition",
            values="value",
        )

    return _asset
