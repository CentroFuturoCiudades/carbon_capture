import ee
import pandas as pd

import dagster as dg
from afolu.assets.common import (
    year_to_band_name,
)
from afolu.assets.constants import LABEL_LIST
from afolu.partitions import wanted_zones_partitions, year_partitions

ins = {
    f"{label}_img": dg.AssetIn(["small", "class_mask", label]) for label in LABEL_LIST
}
ins["grasslands_img"] = dg.AssetIn(["small", "class_mask", "grasslands_merged"])


cross_partition_def = dg.MultiPartitionsDefinition(
    {"year": year_partitions, "zone": wanted_zones_partitions}
)


@dg.asset(
    name="raster",
    key_prefix=["small", "area"],
    ins=ins,
    partitions_def=cross_partition_def,
    io_manager_key="ee_manager",
    group_name="small_area",
)
def area_raster(
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
    year, _ = context.partition_key.split("|")

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

    out_img = ee.image.Image.constant(0).rename("class").uint8()
    for i, label in enumerate(LABEL_LIST):
        out_img = out_img.where(img_map[label].select(band).rename("class"), i + 1)

    return out_img


@dg.asset(
    name="table",
    key_prefix=["small", "area"],
    ins={
        "img": dg.AssetIn(["small", "area", "raster"]),
        "bbox": dg.AssetIn(["small", "bbox", "ee"]),
    },
    partitions_def=cross_partition_def,
    io_manager_key="dataframe_manager",
    group_name="small_area",
)
def area_table(img: ee.image.Image, bbox: ee.geometry.Geometry) -> pd.DataFrame:
    transition_img: ee.image.Image = img.addBands(ee.image.Image.pixelArea()).select(
        ["area", "class"],
    )

    response = transition_img.reduceRegion(
        reducer=(ee.reducer.Reducer.sum().group(groupField=1, groupName="transition")),
        scale=30,
        geometry=bbox,
        maxPixels=int(1e10),
    ).getInfo()

    if response is None:
        err = "No data returned from reduceRegion."
        raise ValueError(err)

    rows = [
        {
            "label": LABEL_LIST[elem["transition"] - 1],
            "area": float(elem["sum"]),
        }
        for elem in response["groups"]
        if elem["transition"] != 0
    ]

    return pd.DataFrame(rows).set_index("label")


@dg.asset(
    name="table_merged",
    key_prefix=["small", "area"],
    ins={
        "table_map": dg.AssetIn(["small", "area", "table"]),
    },
    partitions_def=wanted_zones_partitions,
    io_manager_key="dataframe_manager",
    group_name="small_area",
)
def area_table_merged(table_map: dict[str, pd.DataFrame]) -> pd.DataFrame:
    out = []
    for key, df in table_map.items():
        year, _ = key.split("|")
        temp = df.assign(year=int(year) - 2000)
        out.append(temp)

    return (
        pd.concat(out)
        .pivot_table(index="label", columns="year", values="area")
        .divide(10_000)
    )


@dg.asset(
    name="table_frac",
    key_prefix=["small", "area"],
    ins={
        "table": dg.AssetIn(["small", "area", "table_merged"]),
    },
    partitions_def=wanted_zones_partitions,
    io_manager_key="dataframe_manager",
    group_name="small_area",
)
def area_table_frac(table: pd.DataFrame) -> pd.DataFrame:
    table = table.set_index("label")
    return table.div(table.sum(axis=0), axis=1)
