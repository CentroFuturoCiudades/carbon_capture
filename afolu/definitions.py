from pathlib import Path

import ee
import toml

import dagster as dg
from afolu.defs.managers import (
    DataFrameManager,
    EarthEngineManager,
    FigureManager,
    GeoDataFrameManager,
    JSONManager,
    NumPyManager,
    RasterManager,
    ShapelyManager,
    TextManager,
)
from afolu.defs.resources import (
    AFOLUClassMapResource,
    ConfigResource,
    LabelResource,
    PathResource,
    SelectedAreaResource,
    ZoneBufferResource,
)

ee.Initialize(project="ee-ursa-test")


# # Definitions
# defs = dg.Definitions.merge(
#     dg.Definitions(
#         assets=(
#             list(
#                 dg.load_assets_from_modules(
#                     [
#                         assets.bbox,
#                         assets.class_masks,
#                         assets.load,
#                         assets.emissions,
#                         assets.zones,
#                         # assets.stats,
#                         # assets.rasters,
#                     ],
#                 ),
#             )
#         ),
#         resources=dict(
#             class_map_resource=class_map_resource,
#             path_resource=path_resource,
#             dataframe_manager=dataframe_manager,
#             ee_manager=ee_manager,
#             geodataframe_manager=geodataframe_manager,
#             json_manager=json_manager,
#             numpy_manager=numpy_manager,
#             raster_manager=raster_manager,
#             shapely_manager=shapely_manager,
#             text_manager=text_manager,
#             selected_area_resource=selected_area_resource,
#             zone_buffer_resource=zone_buffer_resource,
#             figure_manager=figure_manager,
#             config_resource=config_resource,
#             **all_resources_map,
#         ),
#     ),
#     assets.plot.defs,
#     assets.large.defs,
#     assets.small.defs,
# )


@dg.definitions
def defs() -> dg.Definitions:
    main_defs = dg.load_from_defs_folder(project_root=Path(__file__).parent.parent)

    # Resources
    config_resource = ConfigResource(fix_settlements=False)

    selected_area_resource = SelectedAreaResource(selected_area="mexico")

    path_resource = PathResource(
        data_path=dg.EnvVar("DATA_PATH"),
        ghsl_path=dg.EnvVar("GHSL_PATH"),
        population_grids_path=dg.EnvVar("POPULATION_GRIDS_PATH"),
        amazonas_path=dg.EnvVar("AMAZONAS_PATH"),
        natural_oceans_path=dg.EnvVar("NATURAL_OCEANS_PATH"),
    )

    with Path("./id_map.toml").open(encoding="utf8") as f:
        config = toml.load(f)

    spec_map = {}
    all_resources_map = {}
    for key, spec in config.items():
        resource = LabelResource(
            ranges=spec.get("ranges"),
        )

        spec_map[key] = resource
        all_resources_map[f"{key}_map"] = resource

    class_map_resource = AFOLUClassMapResource(**spec_map)

    zone_buffer_resource = ZoneBufferResource(
        buffers={"COL+San José del Guaviare": 1_000, "GUY+Georgetown": 5_000},
    )

    # Managers
    dataframe_manager = DataFrameManager(
        path_resource=path_resource,
        extension=".csv",
    )
    ee_manager = EarthEngineManager(
        path_resource=path_resource,
        extension=".json",
    )
    geodataframe_manager = GeoDataFrameManager(
        path_resource=path_resource,
        extension=".gpkg",
    )
    json_manager = JSONManager(
        path_resource=path_resource,
        extension=".json",
    )
    numpy_manager = NumPyManager(
        path_resource=path_resource,
        extension=".npy",
    )
    raster_manager = RasterManager(
        path_resource=path_resource,
        extension=".tif",
    )
    shapely_manager = ShapelyManager(
        path_resource=path_resource,
        extension=".json",
    )
    text_manager = TextManager(
        path_resource=path_resource,
        extension=".txt",
    )
    figure_manager = FigureManager(
        path_resource=path_resource,
        extension=".jpg",
    )

    extra_defs = dg.Definitions(
        resources=dict(
            class_map_resource=class_map_resource,
            path_resource=path_resource,
            dataframe_manager=dataframe_manager,
            ee_manager=ee_manager,
            geodataframe_manager=geodataframe_manager,
            json_manager=json_manager,
            numpy_manager=numpy_manager,
            raster_manager=raster_manager,
            shapely_manager=shapely_manager,
            text_manager=text_manager,
            selected_area_resource=selected_area_resource,
            zone_buffer_resource=zone_buffer_resource,
            figure_manager=figure_manager,
            config_resource=config_resource,
            **all_resources_map,
        ),
    )

    return dg.Definitions.merge(main_defs, extra_defs)
