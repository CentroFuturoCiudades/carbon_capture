from pathlib import Path

import ee
import geopandas as gpd
import rasterio as rio
import rasterio.features as rio_features
import shapely
from pyproj.aoi import AreaOfInterest
from pyproj.database import query_utm_crs_info

import dagster as dg
from afolu.partitions import wanted_zones_partitions
from afolu.resources import PathResource, ZoneBufferResource


# Amazonas
@dg.asset(
    name="shapely",
    key_prefix=["amazon", "bbox"],
    io_manager_key="geodataframe_manager",
    group_name="amazon_bbox",
)
def bbox_amazon(path_resource: PathResource) -> gpd.GeoDataFrame:
    fpath = (
        Path(path_resource.data_path) / "initial" / "sdat_671_1_20250409_130228387.tif"
    )
    with rio.open(fpath) as ds:
        data = ds.read(1)
        crs = ds.crs
        transform = ds.transform

    shapes = rio_features.shapes(data, transform=transform)
    polygons = [shapely.geometry.shape(shape) for shape, value in shapes if value == 1]
    polygons = gpd.GeoSeries(polygons, crs=crs).to_crs("ESRI:102033")
    merged = shapely.union_all(polygons.values)

    if isinstance(merged, shapely.MultiPolygon):
        max_area, max_poly = 0, None
        for poly in merged.geoms:
            area = poly.area
            if area > max_area:
                max_area = area
                max_poly = poly
    elif isinstance(merged, shapely.Polygon):
        max_poly = merged
    else:
        err = f"Expected MultiPolygon or Polygon, got {type(merged)}"
        raise TypeError(err)

    simplified = shapely.simplify(max_poly, tolerance=100)

    if not isinstance(simplified, shapely.Polygon):
        err = f"Expected Polygon, got {type(simplified)}"
        raise TypeError(err)

    return gpd.GeoDataFrame(
        geometry=[simplified],
        crs="ESRI:102033",
    ).to_crs("EPSG:4326")


@dg.asset(
    name="shapely",
    key_prefix=["mexico", "bbox"],
    io_manager_key="geodataframe_manager",
    group_name="mexico_bbox",
)
def bbox_mexico(path_resource: PathResource) -> gpd.GeoDataFrame:
    fpath = Path(path_resource.data_path) / "initial" / "gadm41_MEX.gpkg"
    geom: shapely.MultiPolygon = (
        gpd.read_file(fpath, layer=0)["geometry"].to_crs("EPSG:6372").item()
    )

    max_geom, max_area = None, 0
    for g in geom.geoms:
        area = g.area
        if area > max_area:
            max_geom = g
            max_area = area

    simplified = shapely.simplify(max_geom, tolerance=100)

    if not isinstance(simplified, shapely.Polygon):
        err = f"Expected Polygon, got {type(simplified)}"
        raise TypeError(err)

    return gpd.GeoDataFrame(geometry=[simplified], crs="EPSG:6372").to_crs("EPSG:4326")


@dg.asset(
    name="shapely",
    key_prefix=["small", "bbox"],
    ins={"zone": dg.AssetIn(["small", "zones"])},
    partitions_def=wanted_zones_partitions,
    io_manager_key="geodataframe_manager",
    group_name="small_bbox",
    tags={"partitions": "zone"},
)
def bbox_small(
    context: dg.AssetExecutionContext,
    path_resource: PathResource,
    zone_buffer_resource: ZoneBufferResource,
    zone: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    natural_oceans_path = Path(path_resource.natural_oceans_path)
    oceans_geom = (
        gpd.read_file(natural_oceans_path / "ne_10m_ocean.shp")
        .to_crs("EPSG:4326")["geometry"]
        .item()
    )

    bounds = zone["geometry"].total_bounds

    utm_crs_list = query_utm_crs_info(
        datum_name="WGS 84",
        area_of_interest=AreaOfInterest(
            west_lon_degree=bounds[0],
            south_lat_degree=bounds[1],
            east_lon_degree=bounds[2],
            north_lat_degree=bounds[3],
        ),
    )
    crs = utm_crs_list[0].code
    geom = zone["geometry"].union_all()

    if context.partition_key in zone_buffer_resource.buffers:
        buffer = zone_buffer_resource.buffers[context.partition_key]
    else:
        buffer = 10_000

    return (
        gpd.GeoDataFrame(geometry=[geom], crs="EPSG:4326")
        .to_crs(crs)
        .assign(geometry=lambda df: df["geometry"].simplify(1000).buffer(buffer))
        .to_crs("EPSG:4326")
        .assign(geometry=lambda df: df["geometry"].difference(oceans_geom))
    )


def bbox_ee_factory(
    top_prefix: str,
    partitions_def: dg.PartitionsDefinition | None = None,
) -> dg.AssetsDefinition:
    @dg.asset(
        name="ee",
        key_prefix=[top_prefix, "bbox"],
        ins={"df_bbox": dg.AssetIn([top_prefix, "bbox", "shapely"])},
        partitions_def=partitions_def,
        io_manager_key="ee_manager",
        group_name=f"{top_prefix}_bbox",
        tags={"partitions": "zone"} if partitions_def is not None else None,
    )
    def _asset(df_bbox: gpd.GeoDataFrame) -> ee.geometry.Geometry:
        bbox_shapely = df_bbox["geometry"].item()

        if not isinstance(bbox_shapely, shapely.Polygon):
            err = f"Expected Polygon, got {type(bbox_shapely)}"
            raise TypeError(err)

        return ee.geometry.Geometry.Polygon(
            list(zip(*bbox_shapely.exterior.coords.xy, strict=False)),
        )

    return _asset


dassets = [
    bbox_ee_factory(prefix, partitions_def)
    for prefix, partitions_def in zip(
        ["amazon", "mexico", "small"],
        [None, None, wanted_zones_partitions],
        strict=False,
    )
]
