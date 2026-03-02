from collections.abc import Sequence
from pathlib import Path

import geopandas as gpd
import pandas as pd

import dagster as dg
from afolu.assets.constants import CODE_TO_MEXICO_CITY_MAP
from afolu.partitions import wanted_zones_partitions
from afolu.resources import PathResource

ISO_TO_COUNTRY = {
    "BOL": "Bolivia",
    "BRA": "Brasil",
    "COL": "Colombia",
    "ECU": "Ecuador",
    "GUY": "Guyana",
    "PER": "Peru",
    "SUR": "Suriname",
    "VEN": "Venezuela",
}


ZONE_TO_FUA_NAME_MAP = {
    "PER+San Martín": "PER+Tarapoto",
    "PER+Coronel Portillo": "PER+Pucallpa",
    "VEN+Atures": "VEN+Puerto Ayacucho",
}


ZONE_TO_POLYGON_IDS_MAP = {
    "BOL+Guayaramerín": [1962, 1950],
    "BRA+Barcarena": [1932, 1915, 694, 2324],
    "BRA+Cametá": [1914, 4211],
    "BRA+Igarapé-Miri": [2022],
    "BRA+Nova Ipixuna": [2361],
    "BRA+Rio Preto da Eva": [2160],
    "BRA+São Gabriel da Cachoeira": [2085],
    "COL+Milán - Caquetá": [3768],
    "COL+Mocoa": [1926, 9236],
    "COL+Inírida": [1958],
    "ECU+Macas-Morona-Santiago": [1976, 8857, 3791, 1437, 4948],
    "ECU+Lago Agrio": [1870],
    "PER+Maynas": [1871, 1881],
    "PER+Tambopata": [1856],
    "SUR+Nieuw Nickerie": [28_181],
    "GUY+Lethem": [10_055, 10_054],
}


def get_data_from_fua(
    country_iso: str,
    city_name: str,
    df_fua: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame | None:
    fua_list = df_fua.query(
        f"(Cntry_ISO == '{country_iso}') & (eFUA_name == '{city_name}')",
    )
    if len(fua_list) > 1:
        err = f"Multiple FUA found for {country_iso} {city_name}"
        raise ValueError(err)

    if len(fua_list) == 1:
        return (
            fua_list.filter(["FUA_p_2015", "eFUA_name", "geometry"], axis=1)
            .rename(columns={"FUA_p_2015": "pop", "eFUA_name": "name"})
            .to_crs("EPSG:4326")
        )

    return None


def get_data_from_uc(
    country_iso: str,
    city_name: str,
    df_uc: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame | None:
    country_name = ISO_TO_COUNTRY[country_iso]

    uc_list = df_uc.query(
        f"(GC_CNT_GAD_2025 == '{country_name}') & (GC_UCN_MAI_2025 == '{city_name}')",
    )
    if len(uc_list) > 1:
        err = f"Multiple UC found for {country_iso} {city_name}"
        raise ValueError(err)

    if len(uc_list) == 1:
        return (
            uc_list.filter(["GC_POP_TOT_2025", "GC_UCN_MAI_2025", "geometry"], axis=1)
            .rename(columns={"GC_POP_TOT_2025": "pop", "GC_UCN_MAI_2025": "name"})
            .to_crs("EPSG:4326")
        )

    return None


def get_data_from_polygons(
    polygon_ids: Sequence[int],  # noqa: ARG001
    df_poly: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame | None:
    poly_list = df_poly.query("polygon_id.isin(@polygon_ids)")

    names_filtered = poly_list.query("~name.str.startswith('Cerca')")["name"]

    return gpd.GeoDataFrame(
        pd.Series(
            {
                "pop": poly_list["pop_2020"].sum(),
                "geometry": poly_list["geometry"].union_all(),
                "name": names_filtered.str.cat(sep="+"),
            },
        )
        .to_frame()
        .T,
        geometry="geometry",
        crs=df_poly.crs,
    ).to_crs("EPSG:4326")


def get_data_from_mexico(path_resource: PathResource, city: str) -> gpd.GeoDataFrame:
    population_grids_path = Path(path_resource.population_grids_path)

    city_to_code_map = {value: key for key, value in CODE_TO_MEXICO_CITY_MAP.items()}

    if city not in city_to_code_map:
        err = f"City {city} not found in Mexico city code map"
        raise ValueError(err)

    code = city_to_code_map[city]
    return gpd.read_file(
        population_grids_path
        / "final"
        / "zone_agebs"
        / "shaped"
        / "2020"
        / f"{code}.gpkg",
    ).to_crs("EPSG:4326")[["geometry"]]


@dg.asset(
    name="zones",
    key_prefix="small",
    partitions_def=wanted_zones_partitions,
    io_manager_key="geodataframe_manager",
    group_name="zones",
    tags={"partitions": "zone"},
)
def zones(
    context: dg.AssetExecutionContext,
    path_resource: PathResource,
) -> gpd.GeoDataFrame:
    data_path = Path(path_resource.data_path)
    amazonas_path = Path(path_resource.amazonas_path)
    ghsl_path = Path(path_resource.ghsl_path)

    manual_bounds_path = data_path / "initial" / "boundaries"
    name_list = [path.stem for path in manual_bounds_path.iterdir()]
    if context.partition_key in name_list:
        return gpd.read_file(
            manual_bounds_path / f"{context.partition_key}.gpkg",
        ).to_crs("EPSG:4326")

    country_iso, city = context.partition_key.split("+")

    if country_iso == "MEX":
        return get_data_from_mexico(path_resource, city)

    df_fua = gpd.read_file(
        ghsl_path / "GHS_FUA_UCDB2015_GLOBE_R2019A_54009_1K_V1_0.gpkg",
    )
    df_uc = gpd.read_file(
        ghsl_path / "GHS_UCDB_GLOBE_R2024A.gpkg",
        layer="GHS_UCDB_THEME_GENERAL_CHARACTERISTICS_GLOBE_R2024A",
    )
    df_poly = gpd.read_file(
        amazonas_path / "final" / "final" / "polygons" / "split.gpkg",
    )

    zone = context.partition_key

    if zone in ZONE_TO_POLYGON_IDS_MAP:
        data_polygons = get_data_from_polygons(ZONE_TO_POLYGON_IDS_MAP[zone], df_poly)
        if data_polygons is not None:
            return data_polygons

    if zone in ZONE_TO_FUA_NAME_MAP:
        zone = ZONE_TO_FUA_NAME_MAP[zone]

    country_iso, city_name = zone.split("+")

    data_fua = get_data_from_fua(country_iso, city_name, df_fua)
    if data_fua is not None:
        return data_fua

    data_uc = get_data_from_uc(country_iso, city_name, df_uc)
    if data_uc is not None:
        return data_uc

    err = f"Zone {zone} not found in FUA or UC data"
    raise ValueError(err)
