import ee
import pandas as pd

import dagster as dg
from afolu.defs.assets.common import (
    get_raster_area,
    transition_cube_factory,
    transition_table_fixed_factory,
    transition_table_frac_factory,
    year_to_band_name,
)
from afolu.defs.assets.constants import LABEL_LIST
from afolu.defs.partitions import label_pair_partitions, year_pair_partitions

cross_partitions_def = dg.MultiPartitionsDefinition(
    {
        "label": label_pair_partitions,
        "year": year_pair_partitions,
    },
)


def transition_raster_factory(top_prefix: str) -> dg.AssetsDefinition:
    ins = {
        f"{label}_img": dg.AssetIn([top_prefix, "class_mask", label])
        for label in LABEL_LIST
    }
    ins["grasslands_img"] = dg.AssetIn([top_prefix, "class_mask", "grasslands_merged"])

    @dg.asset(
        name="raster_split",
        key_prefix=[top_prefix, "transition"],
        ins=ins,
        io_manager_key="ee_manager",
        partitions_def=cross_partitions_def,
        group_name=f"{top_prefix}_transition",
    )
    def _asset(
        context: dg.AssetExecutionContext,
        croplands_img: ee.image.Image,
        flooded_img: ee.image.Image,
        forests_mangroves_img: ee.image.Image,
        forests_primary_img: ee.image.Image,
        forests_secondary_img: ee.image.Image,
        grasslands_img: ee.image.Image,
        other_img: ee.image.Image,
        pastures_img: ee.image.Image,
        settlements_img: ee.image.Image,
        shrublands_img: ee.image.Image,
        wetlands_img: ee.image.Image,
    ) -> ee.image.Image:
        label_pair, year_pair = context.partition_key.split("|")

        start_year, end_year = year_pair.split("_")
        start_band = year_to_band_name(start_year)
        end_band = year_to_band_name(end_year)

        start_label, end_label = label_pair.split("-")

        img_map = {
            "croplands": croplands_img,
            "flooded": flooded_img,
            "forests_mangroves": forests_mangroves_img,
            "forests_primary": forests_primary_img,
            "forests_secondary": forests_secondary_img,
            "grasslands": grasslands_img,
            "other": other_img,
            "pastures": pastures_img,
            "settlements": settlements_img,
            "shrublands": shrublands_img,
            "wetlands": wetlands_img,
        }

        a: ee.image.Image = img_map[start_label].select(start_band).rename("class")
        b: ee.image.Image = img_map[end_label].select(end_band).rename("class")

        return a.And(b)

    return _asset


def transition_value_factory(top_prefix: str) -> dg.AssetsDefinition:
    @dg.asset(
        name="value",
        key_prefix=[top_prefix, "transition"],
        ins={
            "raster": dg.AssetIn([top_prefix, "transition", "raster_split"]),
            "bbox": dg.AssetIn([top_prefix, "bbox", "ee"]),
        },
        partitions_def=cross_partitions_def,
        io_manager_key="text_manager",
        group_name=f"{top_prefix}_transition",
    )
    def _asset(raster: ee.image.Image, bbox: ee.geometry.Geometry) -> float:
        return get_raster_area(raster, bbox)

    return _asset


def transition_table_factory(top_prefix: str) -> dg.AssetsDefinition:
    @dg.asset(
        name="table",
        key_prefix=[top_prefix, "transition"],
        ins={"value_map": dg.AssetIn([top_prefix, "transition", "value"])},
        partitions_def=year_pair_partitions,
        io_manager_key="dataframe_manager",
        group_name=f"{top_prefix}_transition",
    )
    def _asset(value_map: dict[str, float]) -> pd.DataFrame:
        out = []
        for key, value in value_map.items():
            label_pair, _ = key.split("|")
            start_label, end_label = label_pair.split("-")
            out.append(
                {
                    "start": start_label,
                    "end": end_label,
                    "total_area": value,
                },
            )
        return (
            pd.DataFrame(out)
            .pivot_table(index="start", columns="end", values="total_area")
            .fillna(0)
        )

    return _asset


dassets = [
    factory(top_prefix)
    for top_prefix in ("amazon", "mexico")
    for factory in (
        transition_raster_factory,
        transition_value_factory,
        transition_table_factory,
        transition_table_fixed_factory,
        transition_table_frac_factory,
        transition_cube_factory,
    )
]
