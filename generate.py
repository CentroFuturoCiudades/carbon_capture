# add path to sispeuede to sys.path in python
import pathlib
import sys

# YOU CAN INSTALL SISEPUEDE OR RUN IT FROM A GIT PATH

path_ssp_git = pathlib.Path("/Users/usuario/git/sisepuede")
if str(path_ssp_git) not in sys.path:
    sys.path.append(str(path_ssp_git))


import warnings

warnings.filterwarnings("ignore")


from typing import *

# import matplotlib.pyplot as plt
# from SISEPUEDE

##########################
#    GLOBAL VARIABLES    #
##########################

# model variables to access
_MODVAR_NAME_EF_CONVERSION = ":math:\\text{CO}_2 Land Use Conversion Emission Factor"
_MODVAR_NAME_SF_FOREST = "Forest Sequestration Emission Factor"
_MODVAR_NAME_SF_FOREST_YOUNG = "Young Secondary Forest Sequestration Emission Factor"
_MODVAR_NAME_SF_LAND_USE = "Land Use Biomass Sequestration Factor"
_MODVARS_ALL_FROM_DB = [
    _MODVAR_NAME_EF_CONVERSION,
    _MODVAR_NAME_SF_FOREST,
    _MODVAR_NAME_SF_FOREST_YOUNG,
    _MODVAR_NAME_SF_LAND_USE,
]

# paths
_PATH_CUR = pathlib.Path(__file__).parents[0]
_PATH_DATA_FROM_URSA_PIPELINE = _PATH_CUR.joinpath("data_monterrey")
_PATH_DATA_FROM_SSP_PIPELINE = _PATH_CUR.joinpath("sisepuede_pipeline_data")


# errors
class InvalidDirectory(Exception):
    pass


##############################
#    SUPPORTING FUNCTIONS    #
##############################


def get_path(
    path: Union[str, pathlib.Path, None],
    path_default: pathlib.Path,
    verify_exists: bool = True,
) -> pathlib.Path:
    """Check an input path and return a default if needed."""
    # check type of default
    if not isinstance(path_default, pathlib.Path):
        tp = str(type(path_default))
        raise TypeError(f"Invalid type '{tp}': path must be of type pathlib.Path.")

    # try formatting input path
    path = pathlib.Path(path) if isinstance(path, str) else path
    path = path_default if not isinstance(path, pathlib.Path) else path

    # verify existence
    if verify_exists and not path.is_dir():
        raise InvalidDirectory(
            f"Directory '{path}' does not exist. Unable to read input files.",
        )

    return path
