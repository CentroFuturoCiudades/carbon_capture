from itertools import product

from afolu.defs.assets.constants import CODE_TO_MEXICO_CITY_MAP, LABEL_LIST
from dagster import StaticPartitionsDefinition

year_partitions = StaticPartitionsDefinition([f"{year}" for year in range(2000, 2023)])

year_pair_partitions = StaticPartitionsDefinition(
    [f"{year}_{year + 1}" for year in range(2000, 2022)],
)

extra_year_pair_partitions = StaticPartitionsDefinition(["2000_2020"])

label_pair_partitions = StaticPartitionsDefinition(
    [f"{comb[0]}-{comb[1]}" for comb in product(LABEL_LIST, LABEL_LIST)],
)

label_partitions = StaticPartitionsDefinition(LABEL_LIST)

five_year_partitions = StaticPartitionsDefinition(
    [f"{year}" for year in range(2000, 2021, 5)],
)

wanted_zones_partitions = StaticPartitionsDefinition(
    [
        "BOL+Cobija",
        "BOL+Guayaramerín",
        "BOL+Riberalta",
        "BOL+Trinidad",
        "BRA+Abaetetuba",
        "BRA+Altamira",
        "BRA+Barcarena",
        "BRA+Belém",
        "BRA+Boa Vista",
        "BRA+Brasileia",
        "BRA+Cametá",
        "BRA+Igarapé-Miri",
        "BRA+Manaus",
        "BRA+Nova Ipixuna",
        "BRA+Paragominas",
        "BRA+Rio Branco",
        "BRA+Rio Preto da Eva",
        "BRA+Santarém",
        "BRA+Palmas",
        "BRA+Parauapebas",
        "BRA+São Luís",
        "BRA+São Gabriel da Cachoeira",
        "COL+Florencia",
        "COL+Milán - Caquetá",
        "COL+Mocoa",
        "COL+San José del Guaviare",
        "COL+Inírida",
        "COL+Leticia",
        "ECU+Macas-Morona-Santiago",
        "ECU+Lago Agrio",
        "ECU+Tena",
        "GUY+Georgetown",
        "GUY+Lethem",
        "PER+Maynas",
        "PER+Tambopata",
        "PER+San Martín",
        "PER+Coronel Portillo",
        "SUR+Nieuw Nickerie",
        "VEN+Atures",
        *[f"MEX+{x}" for x in sorted(CODE_TO_MEXICO_CITY_MAP.values())],
    ],
)


scenario_partitions = StaticPartitionsDefinition(["fast", "normal", "slow"])
