from pathlib import Path

import numpy as np
import pandas as pd

import dagster as dg
from afolu.defs.assets.constants import LABEL_LIST
from afolu.defs.assets.emissions import build_dataset, generate_model_objects
from afolu.defs.partitions import scenario_partitions, wanted_zones_partitions
from afolu.defs.resources import PathResource


def convert_transition_df_to_mat(df: pd.DataFrame) -> np.ndarray:
    transition_mat = np.zeros((len(df), len(LABEL_LIST), len(LABEL_LIST)))
    for idx, row in df.iterrows():
        data = (
            row.rename("value")
            .reset_index()
            .assign(
                index=lambda df: df["index"].str.replace("pij_lndu_", ""),
                start=lambda df: df["index"].str.split("_to_").str[0].str.strip(),
                end=lambda df: df["index"].str.split("_to_").str[1].str.strip(),
            )
            .drop(columns=["index"])
            .pivot_table(index="start", columns="end", values="value")
        )
        data = data.sort_index(axis=0)
        data = data.sort_index(axis=1)
        transition_mat[idx, :, :] = data.to_numpy()

    return transition_mat


def get_area_coefficients(
    df_area: pd.DataFrame,
    *,
    nyears: int | None = None,
) -> dict[str, np.ndarray]:
    df_area = df_area.sort_index()

    if nyears is not None:
        df_area = df_area.iloc[len(df_area) - nyears :]

    X = df_area.index.to_numpy().reshape(-1, 1)
    return {
        label: np.polyfit(X.flatten(), df_area[label].to_numpy(), 1)
        for label in df_area.columns
    }


def derive_coef(x: float, orig_coef: np.ndarray, scale: float) -> np.ndarray:
    new_slope = orig_coef[0] * scale
    new_intersect = np.polyval(orig_coef, x) - new_slope * x
    return np.array([new_slope, new_intersect])


def predict_scaled(x: np.ndarray, orig_coef: np.ndarray, scale: float) -> float:
    coef = derive_coef(x[0], orig_coef, scale)
    return np.polyval(coef, x)


def make_scenario_predictions(
    df_area_frac: pd.DataFrame,
    offset: float,
    scenario: str,
    nyears: int = 20,
) -> np.ndarray:
    coefficients = get_area_coefficients(df_area_frac, nyears=10)
    print(coefficients)

    X = df_area_frac.index.to_numpy()
    X_future = np.arange(X[-1], X[-1] + nyears)
    y = np.zeros((len(X_future), len(LABEL_LIST)))

    for i, label in enumerate(LABEL_LIST):
        coef = coefficients[label]

        if scenario == "normal":
            scale = 1.0
        elif scenario == "fast":
            if coef[0] > 0 and label in [
                "forests_primary",
                "forests_secondary",
                "forests_mangroves",
            ]:
                scale = 1 - offset
            else:
                scale = 1 + offset
        elif scenario == "slow":
            if coef[0] > 0 and label in [
                "forests_primary",
                "forests_secondary",
                "forests_mangroves",
            ]:
                scale = 1 + offset
            else:
                scale = 1 - offset
        else:
            err = f"Unknown scenario: {scenario}"
            raise ValueError(err)

        y[:, i] = predict_scaled(X_future, coef, scale)

    settlement_idx = LABEL_LIST.index("settlements")
    y[:, settlement_idx] += 1 - y.sum(axis=1)
    return y


def adjust_and_predict(
    Q: np.ndarray,
    x0: np.ndarray,
    targets: dict[int, float],
) -> tuple[np.ndarray, np.ndarray]:
    costs = {key: 0 for key in range(len(LABEL_LIST)) if key not in targets}

    vec_infimum = np.array([-999.0])
    vec_supremum = np.array([999.0])

    _, _, model_afolu, *_ = generate_model_objects()
    _, _, xT, *_ = model_afolu.qadj_get_inputs(
        Q,
        x0,
        targets,
        vec_infimum,
        vec_supremum,
    )

    Q_adj = model_afolu.q_adjuster.solve(
        Q,
        x0,
        xT,
        vec_infimum,
        vec_supremum,
        model_afolu.flag_ignore_constraint,
        costs_x=costs,
    )
    x_future = x0 @ Q_adj
    return Q_adj, x_future


def predict_all(
    Q: np.ndarray,
    x: np.ndarray,
    target_arr: np.ndarray,
    nyears: int = 20,
) -> tuple[np.ndarray, np.ndarray]:
    Q_hist, y_hist = [], []

    np.save("./Q.npy", Q)
    np.save("./x.npy", x)
    np.save("./target_arr.npy", target_arr)

    for i in range(nyears):
        targets = {j: target_arr[i, j] for j in range(len(LABEL_LIST))}
        Q, x = adjust_and_predict(Q, x, targets)
        Q_hist.append(Q)
        y_hist.append(x)

    Q_hist = np.array(Q_hist)
    y_hist = np.array(y_hist)

    np.save("./Q_hist.npy", Q_hist)
    np.save("./y_hist.npy", y_hist)

    return Q_hist, y_hist


def build_dataframes(
    df_transition_orig: pd.DataFrame,
    df_area_orig: pd.DataFrame,
    transition_pred: np.ndarray,
    area_pred: np.ndarray,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_area = df_area_orig.copy()
    df_transition = df_transition_orig.copy()

    for row in area_pred:
        df_area.loc[df_area.index[-1] + 1] = row

    for row in transition_pred:
        df_transition.loc[df_transition.index[-1] + 1] = row.reshape(-1)

    df_area = df_area.T.reset_index()
    df_transition = df_transition.reset_index()

    return df_area, df_transition


@dg.multi_asset(
    name="area_multiple",
    ins={
        "df_area": dg.AssetIn(["small", "area", "table_merged"]),
        "transitions": dg.AssetIn(["small", "transition", "cube"]),
    },
    outs={
        "area": dg.AssetOut(
            key=["small", "forecast", "area"],
            io_manager_key="dataframe_manager",
        ),
        "area_frac": dg.AssetOut(
            key=["small", "forecast", "area_frac"],
            io_manager_key="dataframe_manager",
        ),
        "transitions": dg.AssetOut(
            key=["small", "forecast", "transitions"],
            io_manager_key="dataframe_manager",
        ),
    },
    partitions_def=dg.MultiPartitionsDefinition(
        {"scenario": scenario_partitions, "zone": wanted_zones_partitions},
    ),
    group_name="small_forecast",
)
def area_forecast(
    context: dg.AssetExecutionContext,
    transitions: pd.DataFrame,
    df_area: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    scenario, _ = context.partition_key.split("|")

    transitions = transitions.set_index("time_period")
    transition_mat = convert_transition_df_to_mat(transitions)

    df_area = df_area.set_index("label").transpose().sort_index(axis=1)
    df_area.index = df_area.index.astype(int)

    df_area_frac = df_area.div(df_area.sum(axis=1), axis=0)

    offset = 0.15
    nyears = 20
    yhat = make_scenario_predictions(df_area_frac, offset, scenario, nyears=nyears)

    Q = transition_mat[-1]
    x = df_area_frac.iloc[-1].to_numpy()
    Q_hist, y_hist = predict_all(Q, x, yhat)

    total_area_frac, total_transition = build_dataframes(
        transitions,
        df_area_frac,
        Q_hist,
        y_hist,
    )

    total_area = total_area_frac.set_index("label").multiply(df_area.sum(axis=1).mean())

    if "area" in context.selected_output_names:
        yield dg.Output(total_area, output_name="area")

    if "area_frac" in context.selected_output_names:
        yield dg.Output(total_area_frac.set_index("label"), output_name="area_frac")

    if "transitions" in context.selected_output_names:
        yield dg.Output(
            total_transition.set_index("time_period"),
            output_name="transitions",
        )


@dg.asset(
    name="emissions",
    key_prefix=["small", "forecast"],
    ins={
        "areas": dg.AssetIn(["small", "forecast", "area"]),
        "transitions": dg.AssetIn(["small", "forecast", "transitions"]),
    },
    partitions_def=dg.MultiPartitionsDefinition(
        {"scenario": scenario_partitions, "zone": wanted_zones_partitions},
    ),
    io_manager_key="dataframe_manager",
    group_name="small_forecast",
)
def emissions_small(
    context: dg.AssetExecutionContext,
    path_resource: PathResource,
    areas: pd.DataFrame,
    transitions: pd.DataFrame,
) -> pd.DataFrame:
    _, zone = context.partition_key.split("|")
    iso, _ = zone.split("+")

    # Initialize SISEPUEDE objects
    examples, _, model_afolu, regions, time_periods = generate_model_objects()

    temp = areas.set_index("label")

    areas_frac = temp.div(temp.sum(axis=0), axis=1).reset_index(names="label")

    areas.columns = [int(c) if c.isdigit() else c for c in areas.columns]
    areas_frac.columns = [int(c) if c.isdigit() else c for c in areas_frac.columns]

    areas.to_csv("./areas.csv", index=False)
    areas_frac.to_csv("./areas_frac.csv", index=False)
    transitions.to_csv("./transitions.csv", index=False)

    # run model
    dict_ursa_data = {
        "areas": areas,
        "areas_frac": areas_frac,
        "transitions": transitions,
    }
    df_in = build_dataset(
        examples,
        iso,
        model_afolu,
        regions,
        time_periods,
        dict_ursa_data=dict_ursa_data,
        path_ssp_data=Path(path_resource.data_path)
        / "initial"
        / "sisepuede_pipeline_data",
    )

    return model_afolu(
        df_in,
    ).set_index("time_period")
