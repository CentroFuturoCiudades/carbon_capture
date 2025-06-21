import dagster as dg
from afolu.assets.plot import area, transitions

defs = dg.Definitions(
    assets=dg.load_assets_from_modules([area, transitions]),
)
