import dagster as dg
from afolu.assets.small import area_forecast, areas, transitions

defs = dg.Definitions(
    assets=dg.load_assets_from_modules([
        areas,
        # area_forecast,
        transitions,
    ]),
)
