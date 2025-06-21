import dagster as dg


class PathResource(dg.ConfigurableResource):
    ghsl_path: str
    data_path: str
    population_grids_path: str
    amazonas_path: str
    natural_oceans_path: str


class LabelResource(dg.ConfigurableResource):
    ranges: list[list[int]]


class AFOLUClassMapResource(dg.ConfigurableResource):
    croplands: dg.ResourceDependency[LabelResource]
    forests: dg.ResourceDependency[LabelResource]
    grasslands: dg.ResourceDependency[LabelResource]
    grasslands_to_pastures: dg.ResourceDependency[LabelResource]
    wetlands: dg.ResourceDependency[LabelResource]
    forests_mangroves: dg.ResourceDependency[LabelResource]
    settlements: dg.ResourceDependency[LabelResource]
    flooded: dg.ResourceDependency[LabelResource]
    shrublands: dg.ResourceDependency[LabelResource]
    other: dg.ResourceDependency[LabelResource]


class SelectedAreaResource(dg.ConfigurableResource):
    selected_area: str


class ZoneBufferResource(dg.ConfigurableResource):
    buffers: dict[str, float]
