import dagster as dg
from afolu.assets.plot import area, emissions, transitions

defs = dg.Definitions(
    assets=dg.load_assets_from_modules([area, emissions, transitions]),
)
