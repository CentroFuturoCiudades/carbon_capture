LABEL_LIST = (
    "croplands",
    "flooded",
    "forests_mangroves",
    "forests_primary",
    "forests_secondary",
    "grasslands",
    "other",
    "pastures",
    "settlements",
    "shrublands",
    "wetlands",
)


REDUCE_SCALE = 100


COLUMN_NAME_MAP = {
    "croplands": "Cultivos",
    "flooded": "Inundado",
    "forests_primary": "Bosques primarios",
    "forests_secondary": "Bosques secundarios",
    "grasslands": "Pastizales",
    "other": "Otro",
    "pastures": "Pastizales\np/ganado",
    "settlements": "Asentamientos",
    "shrublands": "Matorrales",
    "wetlands": "Humedales",
}

COLOR_MAP_BASE = {
    "croplands": "#B8860B",
    "flooded": "#4169E1",
    "forests_primary": "#006400",
    "forests_secondary": "#228B22",
    "grasslands": "#90EE90",
    "other": "#9932CC",
    "pastures": "#FFD700",
    "settlements": "#808080",
    "shrublands": "#FFA07A",
    "wetlands": "#40E0D0",
}

COLUMN_COLOR_MAP = {
    COLUMN_NAME_MAP[key]: COLOR_MAP_BASE[key] for key in COLUMN_NAME_MAP
}
COLUMN_COLOR_MAP["Total"] = "#000000"
