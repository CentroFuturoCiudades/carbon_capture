import warnings
from typing import *

import pandas as pd
from sisepuede.utilities._toolbox import islistlike

##  SET GLOBALS


##  START FUNCTIONS


##  build input df for areas frac


def get_full_variable(
    construct: "SISEPUEDEDataConstructs",
    modvar_name: str,
    regions: "Regions",
    regions_keep: Union[str, List[str], None] = None,
) -> pd.DataFrame:
    """Read in the historical and projected value for the variable"""

    regions_keep = (
        [regions_keep] if isinstance(regions_keep, (str, int)) else regions_keep
    )
    regions_keep = (
        [regions.return_region_or_iso(x, return_type="iso") for x in regions_keep]
        if islistlike(regions_keep)
        else None
    )

    sub = islistlike(regions_keep)
    sub &= (len(regions_keep) > 0) if sub else False
    dict_subset = (
        None
        if not sub
        else {
            regions.field_iso: regions_keep,
        }
    )

    df_out = construct.read_and_combine_from_output_database(
        modvar_name,
        bound_types="nominal",
        table_types=None,
        dict_subset=dict_subset,
    ).drop(
        columns=[construct.table_types.key, construct.variable_bounds_return_type.key],
    )

    return df_out


def get_vars_from_db(
    construct: "SISEPUEDEDataConstructs",
    modvars: List[str],
    model_attributes: "ModelAttributes",
    regions: "Regions",
    regions_keep: Union[str, List[str], None] = None,
) -> pd.DataFrame:
    """Read in the historical and projected values for database-based inputs"""

    df_out = None

    for modvar in modvars:
        try:
            df_cur = get_full_variable(
                construct,
                modvar,
                regions,
                regions_keep=regions_keep,
            )

        except Exception as e:
            msg = f"""Unable to retrieve variable {modvar} from SISEPUEDEConstructs database: {e}
            """
            warnings.warn(msg)
            continue

        df_out = (
            df_cur
            if df_out is None
            else pd.merge(
                df_out,
                df_cur,
                how="inner",
            )
        )

    return df_out
