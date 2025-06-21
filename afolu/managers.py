import json
import os
from collections.abc import Sequence
from pathlib import Path
from typing import assert_never

import ee
import ee.deserializer
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio as rio
import shapely
from affine import Affine
from matplotlib.figure import Figure
from rasterio.crs import CRS

import dagster as dg
from afolu.resources import PathResource


def process_partition_key(partition_key: str, root_path: Path, extension: str) -> Path:
    partition_key_split = partition_key.split("|")
    final_path = root_path / "/".join(partition_key_split)
    return final_path.with_suffix(final_path.suffix + extension)


def process_multiple_partitions(
    partition_keys: Sequence[str],
    root_path: Path,
    extension: str,
) -> dict[str, Path]:
    path_map = {}
    for key in partition_keys:
        path_map[key] = process_partition_key(key, root_path, extension)
    return path_map


class BaseManager(dg.ConfigurableIOManager):
    extension: str
    path_resource: dg.ResourceDependency[PathResource]

    def _get_path(
        self,
        context: dg.InputContext | dg.OutputContext,
    ) -> Path | dict[str, Path]:
        out_path = Path(self.path_resource.data_path) / "generated"
        fpath = out_path / "/".join(context.asset_key.path)

        if context.has_asset_partitions:
            # Single partition
            if len(context.asset_partition_keys) == 1:
                final_path = process_partition_key(
                    context.asset_partition_keys[0],
                    fpath,
                    self.extension,
                )
            # Multiple partitions
            else:
                final_path = process_multiple_partitions(
                    context.asset_partition_keys,
                    fpath,
                    self.extension,
                )

        # No partitions
        else:
            final_path = fpath.with_suffix(fpath.suffix + self.extension)
        return final_path


class BaseJSONManager(BaseManager):
    def _write_serialized_json(
        self,
        serialized: dict,
        context: dg.OutputContext,
    ) -> None:
        fpath = self._get_path(context)

        if isinstance(fpath, dict):
            err = "JSONManager does not support multiple partitions."
            raise TypeError(err)

        fpath.parent.mkdir(exist_ok=True, parents=True)

        with open(fpath, "w", encoding="utf8") as f:
            json.dump(serialized, f)

    def _read_serialized_json(self, context: dg.InputContext) -> dict:
        fpath = self._get_path(context)

        if isinstance(fpath, dict):
            err = "JSONManager does not support multiple partitions."
            raise TypeError(err)

        with open(fpath, encoding="utf8") as f:
            return json.load(f)


class JSONManager(BaseJSONManager):
    def handle_output(self, context: dg.OutputContext, obj: dict) -> None:
        self._write_serialized_json(obj, context)

    def load_input(self, context: dg.InputContext) -> dict:
        return self._read_serialized_json(context)


class EarthEngineManager(BaseJSONManager):
    def handle_output(
        self,
        context: dg.OutputContext,
        obj: ee.image.Image | ee.geometry.Geometry,
    ) -> None:
        serialized = json.loads(obj.serialize())
        self._write_serialized_json(serialized, context)

    def load_input(
        self,
        context: dg.InputContext,
    ) -> ee.image.Image | ee.geometry.Geometry:
        serialized = self._read_serialized_json(context)
        deserialized = ee.deserializer.decode(serialized)

        if isinstance(deserialized, (ee.image.Image, ee.geometry.Geometry)):
            return deserialized

        err: str = f"Unsupported type: {type(deserialized)}"
        raise TypeError(err)


class ShapelyManager(BaseJSONManager):
    def handle_output(self, context: dg.OutputContext, obj: shapely.Geometry) -> None:
        serialized = json.loads(shapely.to_geojson(obj))
        self._write_serialized_json(serialized, context)

    def load_input(self, context: dg.InputContext) -> shapely.Geometry:
        serialized = self._read_serialized_json(context)
        return shapely.geometry.shape(serialized)


class DataFrameManager(BaseManager):
    def handle_output(self, context: dg.OutputContext, obj: pd.DataFrame) -> None:
        fpath = self._get_path(context)

        if isinstance(fpath, dict):
            err = "DataFrameManager does not support multiple partitions."
            raise TypeError(err)

        fpath.parent.mkdir(exist_ok=True, parents=True)
        obj.to_csv(fpath)

    def load_input(
        self,
        context: dg.InputContext,
    ) -> pd.DataFrame | dict[str, pd.DataFrame]:
        fpath = self._get_path(context)
        if isinstance(fpath, Path):
            return pd.read_csv(fpath)
        return {key: pd.read_csv(p) for key, p in fpath.items()}


class GeoDataFrameManager(BaseManager):
    def handle_output(self, context: dg.OutputContext, obj: gpd.GeoDataFrame) -> None:
        fpath = self._get_path(context)

        if isinstance(fpath, dict):
            err = "GeoDataFrameManager does not support multiple partitions."
            raise TypeError(err)

        fpath.parent.mkdir(exist_ok=True, parents=True)
        obj.to_file(fpath)

    def load_input(self, context: dg.InputContext) -> gpd.GeoDataFrame:
        fpath = self._get_path(context)

        if isinstance(fpath, dict):
            err = "GeoDataFrameManager does not support multiple partitions."
            raise TypeError(err)

        return gpd.read_file(fpath)


class NumPyManager(BaseManager):
    def handle_output(self, context: dg.OutputContext, obj: np.ndarray) -> None:
        fpath = self._get_path(context)

        if isinstance(fpath, dict):
            err = "NumPyManager does not support multiple partitions."
            raise TypeError(err)

        fpath.parent.mkdir(exist_ok=True, parents=True)
        np.save(fpath, obj)

    def load_input(self, context: dg.InputContext) -> np.ndarray:
        fpath = self._get_path(context)

        if isinstance(fpath, dict):
            err = "NumPyManager does not support multiple partitions."
            raise TypeError(err)

        return np.load(fpath)


class RasterManager(BaseManager):
    def handle_output(
        self,
        context: dg.OutputContext,
        obj: tuple[np.ndarray, CRS, Affine],
    ) -> None:
        data, crs, transform = obj
        fpath = self._get_path(context)
        if isinstance(fpath, dict):
            err = "RasterManager does not support multiple partitions."
            raise TypeError(err)

        fpath.parent.mkdir(exist_ok=True, parents=True)

        with rio.open(
            fpath,
            "w",
            driver="GTiff",
            height=data.shape[0],
            width=data.shape[1],
            count=1,
            dtype=data.dtype,
            crs=crs,
            transform=transform,
            compress="lzw",
        ) as ds:
            ds.write(data, 1)

    def load_input(self, context: dg.InputContext) -> tuple[np.ndarray, CRS, Affine]:
        fpath = self._get_path(context)
        with rio.open(fpath) as ds:
            data = ds.read(1)
            crs = ds.crs
            transform = ds.transform

        data = data.squeeze()
        return data, crs, transform


class TextManager(BaseManager):
    def handle_output(self, context: dg.OutputContext, obj: float) -> None:
        fpath = self._get_path(context)

        if isinstance(fpath, dict):
            err = "TextManager does not support multiple partitions."
            raise TypeError(err)

        fpath.parent.mkdir(exist_ok=True, parents=True)
        with open(fpath, "w", encoding="utf8") as f:
            f.write(f"{obj:.6f}")

    def load_input(self, context: dg.InputContext) -> float | dict[str, float]:
        fpath = self._get_path(context)
        if isinstance(fpath, os.PathLike):
            with open(fpath, encoding="utf8") as f:
                return float(f.read())

        elif isinstance(fpath, dict):
            out = {}
            for key, p in fpath.items():
                with open(p, encoding="utf8") as f:
                    out[key] = float(f.read())
            return out

        else:
            assert_never(type(fpath))


class FigureManager(BaseManager):
    def handle_output(self, context: dg.OutputContext, obj: Figure) -> None:
        fpath = self._get_path(context)
        if isinstance(fpath, dict):
            err = "FigureManager does not support multiple partitions."
            raise TypeError(err)
        fpath.parent.mkdir(exist_ok=True, parents=True)

        obj.savefig(fpath, bbox_inches="tight", dpi=300)

    def load_input(self, context: dg.InputContext) -> None:
        err = "FigureManager does not support loading input."
        raise NotImplementedError(err)
