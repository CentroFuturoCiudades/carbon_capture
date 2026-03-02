import ee
import pandas as pd

import dagster as dg


@dg.op
def reduce_image_by_area(img: ee.image.Image, bbox: ee.geometry.Geometry) -> dict:
    transition_img = img.addBands(ee.image.Image.pixelArea()).select(["area", "class"])
    return transition_img.reduceRegion(
        reducer=(ee.reducer.Reducer.sum().group(groupField=1, groupName="transition")),
        scale=100,
        geometry=bbox,
        maxPixels=1e10,
    ).getInfo()


@dg.op(out=dg.Out(io_manager_key="dataframe_manager"))
def reduced_to_table(reduced: dict) -> pd.Series:
    out = (
        pd.DataFrame(reduced["groups"])
        .rename(columns={"sum": "total_area"})
        .assign(transition=lambda df: df.transition.astype(int))
        .set_index("transition")["total_area"]
    )

    if not isinstance(out, pd.Series):
        err = f"Expected Series, got {type(out)}"
        raise TypeError(err)

    return out.divide(1e6).sort_index()


def area_table_factory(
    *,
    name: str,
    key_prefix: str | list[str],
    in_name: str,
    partitions_def: dg.PartitionsDefinition,
    top_prefix: str,
) -> dg.AssetsDefinition:
    @dg.graph_asset(
        name=name,
        key_prefix=key_prefix,
        ins={
            "transition_img": dg.AssetIn(in_name),
            "bbox": dg.AssetIn([top_prefix, "bbox", "ee"]),
        },
        partitions_def=partitions_def,
    )
    def _asset(
        transition_img: ee.image.Image,
        bbox: ee.geometry.Geometry,
    ) -> pd.DataFrame:
        return reduced_to_table(reduce_image_by_area(transition_img, bbox))

    return _asset
