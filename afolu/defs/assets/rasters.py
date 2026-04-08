import tempfile
from pathlib import Path

import ee
import geemap
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio as rio
import rasterio.features as rio_features
import rasterio.mask as rio_mask
from affine import Affine
from rasterio.crs import CRS
from shapely.geometry import shape

import dagster as dg
from afolu.defs.partitions import wanted_zones_partitions
from afolu.defs.resources import PathResource


def small_raster_factory(name: str, subdir: str) -> dg.AssetsDefinition:
    @dg.asset(
        name=name,
        key_prefix=["small", "raster"],
        partitions_def=wanted_zones_partitions,
        ins={"df_bbox": dg.AssetIn(["small", "bbox", "shapely"])},
        io_manager_key="raster_manager",
        group_name="small_raster",
    )
    def _asset(
        path_resource: PathResource,
        df_bbox: gpd.GeoDataFrame,
    ) -> tuple[np.ndarray, CRS, Affine]:
        bbox = df_bbox.to_crs("ESRI:54009")["geometry"].item()

        pop_dir_path = Path(path_resource.ghsl_path) / subdir

        crs, transform = None, None
        arr = []
        for year in range(2000, 2021, 5):
            with rio.open(pop_dir_path / f"{year}.tif") as ds:
                masked, transform = rio_mask.mask(ds, [bbox], crop=True, nodata=0)
                masked = masked.squeeze()
                crs = ds.crs

                if not isinstance(masked, np.ndarray):
                    err = f"Expected a 2D array, got {type(masked)}"
                    raise TypeError(err)

                if not isinstance(crs, CRS):
                    err = f"Expected a CRS object, got {type(crs)}"
                    raise TypeError(err)

                if not isinstance(transform, Affine):
                    err = f"Expected an Affine transform, got {type(transform)}"
                    raise TypeError(err)

                arr.append(masked)

        if crs is None:
            err = "CRS is None, check the raster file."
            raise ValueError(err)

        if transform is None:
            err = "Transform is None, check the raster file."
            raise ValueError(err)

        return np.array(arr), crs, transform

    return _asset


@dg.asset(
    name="built_binary",
    key_prefix=["small", "raster"],
    partitions_def=wanted_zones_partitions,
    ins={
        "bbox": dg.AssetIn(["small", "bbox", "ee"]),
        "settlements_mask": dg.AssetIn(["small", "class_mask", "settlements"]),
    },
    io_manager_key="raster_manager",
    group_name="small_raster",
)
def built_binary_raster(
    bbox: ee.geometry.Geometry,
    settlements_mask: ee.image.Image,
) -> tuple[np.ndarray, CRS, Affine]:
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir) / "settlements.tif"
        geemap.download_ee_image(
            image=settlements_mask,
            filename=tmp_path,
            region=bbox,
            crs="EPSG:4326",
            scale=30,
            resampling="near",
        )

        with rio.open(tmp_path) as ds:
            arr = ds.read().squeeze()
            crs = ds.crs
            transform = ds.transform

    return arr, crs, transform


@dg.asset(
    name="built_binary_polygons",
    key_prefix=["small", "raster"],
    partitions_def=wanted_zones_partitions,
    ins={"built_binary_raster": dg.AssetIn(["small", "raster", "built_binary"])},
    io_manager_key="geodataframe_manager",
    group_name="small_raster",
)
def built_binary_polygons(
    built_binary_raster: tuple[np.ndarray, CRS, Affine],
) -> gpd.GeoDataFrame:
    arr, crs, transform = built_binary_raster

    shapes = []
    for i in range(arr.shape[0]):
        for shape_json, value in rio_features.shapes(arr[i], transform=transform):
            if value == 1:
                shapes.append({"year": i + 2000, "geometry": shape(shape_json)})

    return gpd.GeoDataFrame(
        pd.DataFrame(shapes),
        geometry="geometry",
        crs=crs,
    ).dissolve("year")


dassets = [
    small_raster_factory("population", "POP_1000"),
    small_raster_factory("built_area", "BUILT_100"),
]
