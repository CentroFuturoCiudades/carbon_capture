import ee

import dagster as dg
from afolu.defs.partitions import wanted_zones_partitions
from afolu.defs.resources import AFOLUClassMapResource, LabelResource


def class_mask_factory(
    top_prefix: str,
    class_name: str,
    partitions_def: dg.PartitionsDefinition | None = None,
) -> dg.AssetsDefinition:
    @dg.asset(
        name=class_name,
        key_prefix=[top_prefix, "class_mask"],
        ins={
            "bbox": dg.AssetIn([top_prefix, "bbox", "ee"]),
            "glc30": dg.AssetIn([top_prefix, "glc30"]),
        },
        partitions_def=partitions_def,
        io_manager_key="ee_manager",
        group_name=f"{top_prefix}_class_mask",
        tags={"partitions": "zone"} if partitions_def is not None else None,
    )
    def _asset(
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

    return _asset


def forests_primary_factory(
    top_prefix: str,
    partitions_def: dg.PartitionsDefinition | None = None,
) -> dg.AssetsDefinition:
    @dg.asset(
        name="forests_primary",
        key_prefix=[top_prefix, "class_mask"],
        ins={
            "forests_img": dg.AssetIn([top_prefix, "class_mask", "forests"]),
            "forests_mask": dg.AssetIn([top_prefix, "forests_mask"]),
        },
        partitions_def=partitions_def,
        io_manager_key="ee_manager",
        group_name=f"{top_prefix}_class_mask",
        tags={"partitions": "zone"} if partitions_def is not None else None,
    )
    def _asset(
        forests_img: ee.image.Image,
        forests_mask: ee.image.Image,
    ) -> ee.image.Image:
        return forests_img.And(forests_mask)

    return _asset


def forests_secondary_factory(
    top_prefix: str,
    partitions_def: dg.PartitionsDefinition | None = None,
) -> dg.AssetsDefinition:
    @dg.asset(
        name="forests_secondary",
        key_prefix=[top_prefix, "class_mask"],
        ins={
            "forests_img": dg.AssetIn([top_prefix, "class_mask", "forests"]),
            "forests_primary_img": dg.AssetIn(
                [top_prefix, "class_mask", "forests_primary"],
            ),
        },
        partitions_def=partitions_def,
        io_manager_key="ee_manager",
        group_name=f"{top_prefix}_class_mask",
        tags={"partitions": "zone"} if partitions_def is not None else None,
    )
    def _asset(
        forests_img: ee.image.Image,
        forests_primary_img: ee.image.Image,
    ) -> ee.image.Image:
        return forests_img.And(forests_primary_img.Not())

    return _asset


def pastures_factory(
    top_prefix: str,
    partitions_def: dg.PartitionsDefinition | None = None,
) -> dg.AssetsDefinition:
    @dg.asset(
        key=[top_prefix, "class_mask", "pastures"],
        ins={
            "grasslands_to_pastures_img": dg.AssetIn(
                [top_prefix, "class_mask", "grasslands_to_pastures"],
            ),
            "pastures_random_mask": dg.AssetIn(
                [top_prefix, "pastures_random_mask"],
            ),
        },
        partitions_def=partitions_def,
        io_manager_key="ee_manager",
        group_name=f"{top_prefix}_class_mask",
        tags={"partitions": "zone"} if partitions_def is not None else None,
    )
    def _asset(
        grasslands_to_pastures_img: ee.image.Image,
        pastures_random_mask: ee.image.Image,
    ) -> ee.image.Image:
        return grasslands_to_pastures_img.And(pastures_random_mask)

    return _asset


def grasslands_merged_factory(
    top_prefix: str,
    partitions_def: dg.PartitionsDefinition | None = None,
) -> dg.AssetsDefinition:
    @dg.asset(
        name="grasslands_merged",
        key_prefix=[top_prefix, "class_mask"],
        ins={
            "grasslands_to_pastures_img": dg.AssetIn(
                [top_prefix, "class_mask", "grasslands_to_pastures"],
            ),
            "grasslands_img": dg.AssetIn([top_prefix, "class_mask", "grasslands"]),
            "pastures_random_mask": dg.AssetIn(
                [top_prefix, "pastures_random_mask"],
            ),
        },
        partitions_def=partitions_def,
        io_manager_key="ee_manager",
        group_name=f"{top_prefix}_class_mask",
        tags={"partitions": "zone"} if partitions_def is not None else None,
    )
    def _asset(
        grasslands_to_pastures_img: ee.image.Image,
        grasslands_img: ee.image.Image,
        pastures_random_mask: ee.image.Image,
    ) -> ee.image.Image:
        return grasslands_to_pastures_img.And(
            pastures_random_mask.Not(),
        ).Or(grasslands_img)

    return _asset


def foo(img: ee.image.Image, prev: ee.image.Image) -> ee.image.Image:
    return img.bitwiseOr(prev)


# def settlements_fixed_factory(
#     top_prefix: str,
#     partitions_def: dg.PartitionsDefinition | None = None,
# ) -> dg.AssetsDefinition:
#     @dg.asset(
#         name="settlements_fixed",
#         key_prefix=[top_prefix, "class_mask"],
#         ins={
#             "settlements_mask": dg.AssetIn([top_prefix, "class_mask", "settlements"]),
#         },
#         partitions_def=partitions_def,
#         io_manager_key="ee_manager",
#         group_name=f"{top_prefix}_class_mask",
#     )
#     def _asset(settlements_mask: ee.image.Image) -> ee.image.Image:
#         final_idx = 25
#         bands = []
#         for end_idx in range(2, final_idx):
#             col = ee.imagecollection.ImageCollection.fromImages(
#                 [settlements_mask.select(f"b{i}") for i in range(1, end_idx)],
#             )
#             res = ee.image.Image(col.iterate(foo, ee.image.Image.constant(0)))
#             bands.append(res)

#         return (
#             ee.imagecollection.ImageCollection.fromImages(bands)
#             .toBands()
#             .rename([f"b{i}" for i in range(1, final_idx - 1)])
#         )

#     return _asset


# def final_mask_factory(
#     top_prefix: str,
#     class_name: str,
#     partitions_def: dg.PartitionsDefinition | None,
# ) -> dg.AssetsDefinition:
#     @dg.asset(
#         name=f"{class_name}_final",
#         key_prefix=[top_prefix, "class_mask"],
#         ins={
#             "class_mask": dg.AssetIn([top_prefix, "class_mask", class_name]),
#             "settlements_mask": dg.AssetIn(
#                 [top_prefix, "class_mask", "settlements_fixed"],
#             ),
#         },
#         partitions_def=partitions_def,
#         io_manager_key="ee_manager",
#         group_name=f"{top_prefix}_class_mask",
#     )
#     def _asset(
#         config_resource: ConfigResource,
#         class_mask: ee.image.Image,
#         settlements_mask: ee.image.Image,
#     ) -> ee.image.Image:
#         if config_resource.fix_settlements:
#             return class_mask.And(settlements_mask.Not())
#         return class_mask

#     return _asset


# def settlements_final_mask_factory(
#     top_prefix: str,
#     partitions_def: dg.PartitionsDefinition | None,
# ) -> dg.AssetsDefinition:
#     @dg.asset(
#         name="settlements_final",
#         key_prefix=[top_prefix, "class_mask"],
#         ins={
#             "settlements_mask": dg.AssetIn(
#                 [top_prefix, "class_mask", "settlements_fixed"],
#             ),
#         },
#         partitions_def=partitions_def,
#         io_manager_key="ee_manager",
#         group_name=f"{top_prefix}_class_mask",
#     )
#     def _asset(settlements_mask: ee.image.Image) -> ee.image.Image:
#         return settlements_mask

#     return _asset


assets = (
    [
        class_mask_factory(top_prefix, class_name, partitions_def)
        for class_name in [
            "croplands",
            "forests_mangroves",
            "forests",
            "grasslands",
            "grasslands_to_pastures",
            "wetlands",
            "settlements",
            "flooded",
            "shrublands",
            "other",
        ]
        for top_prefix, partitions_def in zip(
            ["amazon", "mexico", "small"],
            [None, None, wanted_zones_partitions],
            strict=True,
        )
    ]
    + [
        factory(top_prefix, partitions_def)
        for factory in (
            forests_primary_factory,
            forests_secondary_factory,
            pastures_factory,
            grasslands_merged_factory,
        )
        for top_prefix, partitions_def in zip(
            ["amazon", "mexico", "small"],
            [None, None, wanted_zones_partitions],
            strict=True,
        )
    ]
    # + [
    #     final_mask_factory(top_prefix, class_name, partitions_def)
    #     for class_name in [
    #         "croplands",
    #         "flooded",
    #         "forests_mangroves",
    #         "forests_primary",
    #         "forests_secondary",
    #         "grasslands_merged",
    #         "other",
    #         "pastures",
    #         "shrublands",
    #         "wetlands",
    #     ]
    #     for top_prefix, partitions_def in zip(
    #         ["amazon", "mexico", "small"],
    #         [None, None, wanted_zones_partitions],
    #         strict=True,
    #     )
    # ]
    # + [
    #     settlements_final_mask_factory(top_prefix, partitions_def)
    #     for top_prefix, partitions_def in zip(
    #         ["amazon", "mexico", "small"],
    #         [None, None, wanted_zones_partitions],
    #         strict=True,
    #     )
    # ]
)
