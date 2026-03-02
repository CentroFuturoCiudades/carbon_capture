from pathlib import Path

import geopandas as gpd
import pandas as pd
import rasterio as rio
import rasterio.mask as rio_mask

import dagster as dg
from afolu.defs.partitions import wanted_zones_partitions
from afolu.defs.resources import PathResource


@dg.asset(
    name="population",
    key_prefix=["small", "stats"],
    partitions_def=wanted_zones_partitions,
    ins={"df_bbox": dg.AssetIn(["small", "bbox", "shapely"])},
    io_manager_key="dataframe_manager",
    group_name="small_stats",
)
def population(path_resource: PathResource, df_bbox: gpd.GeoDataFrame) -> pd.DataFrame:
    bbox = df_bbox.to_crs("ESRI:54009")["geometry"].item()

    pop_dir_path = Path(path_resource.ghsl_path) / "POP_1000"

    pops = []
    for year in range(2000, 2021, 5):
        with rio.open(pop_dir_path / f"{year}.tif") as ds:
            masked, _ = rio_mask.mask(ds, [bbox], crop=True, nodata=0)
            pops.append(
                {
                    "time_period": year,
                    "population": masked.sum(),
                },
            )

    return pd.DataFrame(pops).set_index("time_period")


@dg.asset(
    name="built_area",
    key_prefix=["small", "stats"],
    partitions_def=wanted_zones_partitions,
    ins={"area": dg.AssetIn(["small", "area", "table_merged"])},
    io_manager_key="dataframe_manager",
    group_name="small_stats",
)
def built_area(area: pd.DataFrame) -> pd.DataFrame:
    out = area.set_index("label").T
    out.index = out.index.astype(int) + 2000
    out.index.name = "time_period"
    return out.filter(["settlements"])
