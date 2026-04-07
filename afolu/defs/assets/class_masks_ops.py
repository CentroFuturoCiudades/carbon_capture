import ee
import numpy as np
import pandas as pd

import dagster as dg
from afolu.defs.assets.common import LABEL_LIST, year_to_band_name
from afolu.defs.partitions import wanted_zones_partitions
from afolu.defs.resources import AFOLUClassMapResource, LabelResource


def class_mask_op_factory(
    class_name: str,
) -> dg.OpDefinition:
    @dg.op(
        name=f"generate_{class_name}_mask",
    )
    def _op(
        class_map_resource: AFOLUClassMapResource,
        glc30: ee.image.Image,
        bbox: ee.geometry.Geometry,
    ) -> ee.image.Image:
        label_spec: LabelResource = getattr(class_map_resource, class_name)

        mask = (
            ee.image.Image.constant([0] * 23)
            .rename([f"b{i}" for i in range(1, 24)])
            .clip(bbox)
        )

        for label_range in label_spec.ranges:
            if label_range[0] == label_range[1]:
                temp_mask = glc30.eq(label_range[0])
            else:
                temp_mask = glc30.gte(label_range[0]).And(glc30.lte(label_range[1]))
            mask = mask.Or(temp_mask)

        return mask

    return _op


@dg.op
def generate_random_grasslands_to_pastures_discriminator(
    bbox: ee.geometry.Geometry,
    glc30: ee.image.Image,
) -> ee.Image:
    proj = glc30.projection().getInfo()

    if not isinstance(proj, dict):
        err = f"Expected dict, got {type(proj)}"
        raise TypeError(err)

    return (
        ee.image.Image.random(42)
        .reproject(crs=proj["crs"], crsTransform=proj["transform"])
        .lte(0.4)
        .clip(bbox)
    )


@dg.op
def merge_grasslands(
    grasslands_to_pastures_img: ee.Image,
    grasslands_img: ee.Image,
    pastures_random_mask: ee.Image,
) -> ee.Image:
    return grasslands_to_pastures_img.And(
        pastures_random_mask.Not(),
    ).Or(grasslands_img)


@dg.op
def get_forest_discriminator(bbox: ee.geometry.Geometry) -> ee.image.Image:
    return (
        ee.imagecollection.ImageCollection(
            "NASA/ORNL/global_forest_classification_2020/V1",
        )
        .filterBounds(bbox)
        .mode()
        .clip(bbox)
        .unmask(0)
        .eq(1)
    )


@dg.op
def get_forests_primary_mask(
    forests_img: ee.image.Image,
    forests_discriminator: ee.Image,
) -> ee.image.Image:
    return forests_img.And(forests_discriminator)


@dg.op
def get_forests_secondary_mask(
    forests_img: ee.Image,
    forests_discriminator: ee.Image,
) -> ee.image.Image:
    return forests_img.And(forests_discriminator.Not())


@dg.op
def get_pastures_mask(
    grasslands_to_pastures_img: ee.image.Image,
    pastures_random_discriminator: ee.image.Image,
) -> ee.image.Image:
    return grasslands_to_pastures_img.And(pastures_random_discriminator)


@dg.op
def generate_transition_label_map() -> dict[str, list[str]]:
    multiplier = 10 ** np.ceil(np.log10(len(LABEL_LIST)))

    out = {}
    for i, start_label in enumerate(LABEL_LIST):
        for j, end_label in enumerate(LABEL_LIST):
            key = int(i * multiplier + j)
            if key in out:
                err = f"Key {key} already exists in the dictionary."
                raise ValueError(err)
            out[str(key)] = [start_label, end_label]

    return out


def transition_raster_op_factory(start_year: int) -> dg.OpDefinition:
    @dg.op(name=f"generate_transition_raster_{start_year}")
    def _op(
        bbox: ee.geometry.Geometry,
        transition_label_map: dict[str, list[str]],
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
        end_year = start_year + 1
        start_band = year_to_band_name(start_year)
        end_band = year_to_band_name(end_year)

        masked = ee.image.Image.constant(0).rename("class").uint8().clip(bbox)

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

        transition_label_map_inv = {
            tuple(value): int(key) for key, value in transition_label_map.items()
        }

        for start_label, start_img in img_map.items():
            for end_label, end_img in img_map.items():
                a: ee.image.Image = start_img.select(start_band).rename("class")
                b: ee.image.Image = end_img.select(end_band).rename("class")

                masked = masked.where(
                    a.And(b),
                    transition_label_map_inv[(start_label, end_label)],
                )

        return masked

    return _op


@dg.op
def convert_raster_to_table(
    raster: ee.image.Image,
    bbox: ee.geometry.Geometry,
    transition_label_map: dict[str, list[str]],
) -> pd.DataFrame:
    transition_img: ee.image.Image = raster.addBands(
        ee.image.Image.pixelArea(),
    ).select(
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
            "label": transition_label_map[str(elem["transition"])],
            "area": float(elem["sum"]),
        }
        for elem in response["groups"]
    ]

    out = (
        pd.DataFrame(rows)
        .assign(
            start=lambda df: df["label"].str[0],
            end=lambda df: df["label"].str[1],
        )
        .drop(columns=["label"])
        .pivot_table(index="start", columns="end", values="area")
        .fillna(0)
    )

    for label in LABEL_LIST:
        if label not in out.index:
            out.loc[label] = 0

        if label not in out.columns:
            out[label] = 0

    return out.sort_index(axis=0).sort_index(axis=1)


@dg.op
def fix_transition_table(table: pd.DataFrame) -> pd.DataFrame:
    table = table.set_index("start")

    for start in LABEL_LIST:
        if start == "forests_primary":
            continue

        table.loc[start, "forests_secondary"] = np.nansum(
            [
                table.loc[start, "forests_secondary"],
                table.loc[start, "forests_primary"],
            ],
        )  # ty:ignore[no-matching-overload]
        table.loc[start, "forests_primary"] = np.nan

    return table.fillna(0)


@dg.op
def make_transition_table_fractional(cross_fixed: pd.DataFrame) -> pd.DataFrame:
    cross_fixed = cross_fixed.set_index("start")

    zero_rows = cross_fixed.index[cross_fixed.sum(axis=1) == 0]
    for elem in zero_rows:
        cross_fixed.loc[elem, elem] = 1

    return cross_fixed.divide(cross_fixed.sum(axis=1), axis=0)


@dg.op(
    ins={f"transition_table_{year}": dg.In() for year in range(2000, 2022)},
    out=dg.Out(io_manager_key="dataframe_manager"),
)
def merge_transition_tables(**tables: pd.DataFrame) -> pd.DataFrame:
    table_frac_map = {}
    for key, df in tables.items():
        start_year = int(key.replace("transition_table_", ""))
        end_year = start_year + 1
        table_frac_map[f"{start_year}_{end_year}"] = df

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
        table = table_frac_map[period]
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


@dg.op
def reduce_area_raster_to_table(
    img: ee.image.Image, bbox: ee.geometry.Geometry
) -> pd.DataFrame:
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


@dg.op(
    ins={f"area_raster_{year}": dg.In() for year in range(2000, 2023)},
    out=dg.Out(io_manager_key="dataframe_manager"),
)
def merge_area_tables(**tables: pd.DataFrame) -> pd.DataFrame:
    out = []
    for key, df in tables.items():
        year = key.replace("area_raster_", "")
        temp = df.assign(year=int(year) - 2000)
        out.append(temp)

    out = (
        pd.concat(out)
        .pivot_table(index="label", columns="year", values="area")
        .divide(10_000)
    )

    for label in LABEL_LIST:
        if label not in out.index:
            out.loc[label] = 0.1

    return out.sort_index().sort_index(axis=1)


@dg.op
def load_glc30(bbox: ee.geometry.Geometry) -> ee.image.Image:
    return (
        ee.imagecollection.ImageCollection(
            "projects/sat-io/open-datasets/GLC-FCS30D/annual",
        )
        .filterBounds(bbox)
        .mode()
        .clip(bbox)
    )


def area_raster_factory(year: int) -> dg.OpDefinition:
    @dg.op(name=f"generate_area_raster_{year}")
    def _op(
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

    return _op


@dg.graph_multi_asset(
    ins={
        "bbox": dg.AssetIn(["small", "bbox", "ee"]),
    },
    outs={"area_raster": dg.AssetOut(), "transition_cube": dg.AssetOut()},
    partitions_def=wanted_zones_partitions,
    group_name="small_class_mask_graph",
)
def foo(bbox: ee.Geometry) -> ee.Image:
    glc30 = load_glc30(bbox)
    transition_label_map = generate_transition_label_map()

    grasslands_base = class_mask_op_factory("grasslands")(glc30, bbox)
    grasslands_to_pastures_mask = class_mask_op_factory("grasslands_to_pastures")(
        glc30, bbox
    )
    random_grasslands_to_pastures_discriminator = (
        generate_random_grasslands_to_pastures_discriminator(
            bbox,
            glc30,
        )
    )
    grasslands = merge_grasslands(
        grasslands_to_pastures_mask,
        grasslands_base,
        random_grasslands_to_pastures_discriminator,
    )
    pastures = get_pastures_mask(
        grasslands_to_pastures_mask,
        random_grasslands_to_pastures_discriminator,
    )

    forests_base = class_mask_op_factory("forests")(glc30, bbox)
    forests_discriminator = get_forest_discriminator(bbox)
    forests_primary = get_forests_primary_mask(forests_base, forests_discriminator)
    forests_secondary = get_forests_secondary_mask(forests_base, forests_discriminator)

    croplands = class_mask_op_factory("croplands")(glc30, bbox)
    flooded = class_mask_op_factory("flooded")(glc30, bbox)
    forests_mangroves = class_mask_op_factory("forests_mangroves")(glc30, bbox)
    other = class_mask_op_factory("other")(glc30, bbox)
    settlements = class_mask_op_factory("settlements")(glc30, bbox)
    shrublands = class_mask_op_factory("shrublands")(glc30, bbox)
    wetlands = class_mask_op_factory("wetlands")(glc30, bbox)

    transition_tables = {}
    for start_year in range(2000, 2022):
        transition_raster = transition_raster_op_factory(start_year)(
            bbox=bbox,
            transition_label_map=transition_label_map,
            croplands_img=croplands,
            flooded_img=flooded,
            forests_mangroves_img=forests_mangroves,
            forests_primary_img=forests_primary,
            forests_secondary_img=forests_secondary,
            grasslands_img=grasslands,
            other_img=other,
            pastures_img=pastures,
            settlements_img=settlements,
            shrublands_img=shrublands,
            wetlands_img=wetlands,
        )
        transition_tables[f"transition_table_{start_year}"] = convert_raster_to_table(
            transition_raster, bbox, transition_label_map
        )

    area_rasters = {}
    for year in range(2000, 2023):
        area_raster = area_raster_factory(year)(
            croplands_img=croplands,
            flooded_img=flooded,
            forests_mangroves_img=forests_mangroves,
            forests_primary_img=forests_primary,
            forests_secondary_img=forests_secondary,
            grasslands_img=grasslands,
            other_img=other,
            pastures_img=pastures,
            settlements_img=settlements,
            shrublands_img=shrublands,
            wetlands_img=wetlands,
        )
        area_rasters[f"area_raster_{year}"] = reduce_area_raster_to_table(
            area_raster, bbox
        )

    return {
        "transition_cube": merge_transition_tables(**transition_tables),
        "area_raster": merge_area_tables(**area_rasters),
    }
