import ee
import pandas as pd

import dagster as dg
from afolu.defs.assets.common import get_raster_area, year_to_band_name
from afolu.defs.assets.constants import LABEL_LIST
from afolu.defs.partitions import label_partitions, year_partitions


def area_raster_factory(top_prefix: str) -> dg.AssetsDefinition:
    ins = {
        f"{label}_img": dg.AssetIn([top_prefix, "class_mask", label])
        for label in LABEL_LIST
    }
    ins["grasslands_img"] = dg.AssetIn([top_prefix, "class_mask", "grasslands_merged"])

    @dg.asset(
        name="raster",
        key_prefix=[top_prefix, "area"],
        ins=ins,
        partitions_def=dg.MultiPartitionsDefinition(
            {"year": year_partitions, "label": label_partitions},
        ),
        io_manager_key="ee_manager",
        group_name=f"{top_prefix}_area",
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
        label, year = context.partition_key.split("|")
        band = year_to_band_name(year)

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

        return img_map[label].select(band).rename("class")

    return _asset


def area_value_factory(top_prefix: str) -> dg.AssetsDefinition:
    @dg.asset(
        name="value",
        key_prefix=[top_prefix, "area"],
        ins={
            "raster": dg.AssetIn([top_prefix, "area", "raster"]),
            "bbox": dg.AssetIn([top_prefix, "bbox", "ee"]),
        },
        partitions_def=dg.MultiPartitionsDefinition(
            {
                "year": year_partitions,
                "label": label_partitions,
            },
        ),
        io_manager_key="text_manager",
        group_name=f"{top_prefix}_area",
    )
    def _asset(raster: ee.image.Image, bbox: ee.geometry.Geometry) -> float:
        return get_raster_area(raster, bbox)

    return _asset


def area_table_factory(top_prefix: str) -> dg.AssetsDefinition:
    @dg.asset(
        name="table",
        key_prefix=[top_prefix, "area"],
        ins={
            "value_map": dg.AssetIn([top_prefix, "area", "value"]),
        },
        io_manager_key="dataframe_manager",
        group_name=f"{top_prefix}_area",
    )
    def _asset(value_map: dict[str, float]) -> pd.DataFrame:
        rows = []
        for key, area in value_map.items():
            label, year = key.split("|")
            rows.append(
                {
                    "label": label,
                    "year": int(year) - 2000,
                    "area": float(area) / 10_000,
                },
            )
        return pd.DataFrame(rows).pivot_table(
            index="label",
            columns="year",
            values="area",
        )

    return _asset


def area_table_frac_factory(top_prefix: str) -> dg.AssetsDefinition:
    @dg.asset(
        name="table_frac",
        key_prefix=[top_prefix, "area"],
        ins={
            "table": dg.AssetIn([top_prefix, "area", "table"]),
        },
        io_manager_key="dataframe_manager",
        group_name=f"{top_prefix}_area",
    )
    def _asset(table: pd.DataFrame) -> pd.DataFrame:
        table = table.set_index("label")
        return table.div(table.sum(axis=0), axis=1)

    return _asset


dassets = [
    factory(top_prefix)
    for factory in (
        area_raster_factory,
        area_value_factory,
        area_table_factory,
        area_table_frac_factory,
    )
    for top_prefix in ["amazon", "mexico"]
]
