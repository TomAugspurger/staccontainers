"""
staccontainers: Convert STAC objects to data containers.

This library makes it easy to go from STAC objects (Items, Assets, ItemCollections, etc.)
to data containers (xarray.Dataset, geopandas.GeoDataFrame, etc.)
"""
__version__ = "0.1.0"
import enum
from typing import TYPE_CHECKING, Union
import functools
import importlib
import pystac
import pystac_client
import requests


if TYPE_CHECKING:
    import geopandas
    import dask_geopandas
    import xarray


class _OptionalDependencies(str, enum.Enum):
    stackstac = "stackstac"
    geopandas = "geopandas"
    dask_geopandas = "dask_geopandas"
    xarray = "xarray"
    planetary_computer = "planetary_computer"
    pandas = "pandas"


def _import_optional_dependency(name):
    try:
        module = importlib.import_module(name)
    except ImportError as e:
        raise ImportError(f"Missing optional dependency f'{name}") from e
    return module


@functools.singledispatch
def to_xarray(item, **kwargs) -> "xarray.Dataset":
    raise TypeError


@functools.singledispatch
def to_geopandas(item, **kwargs) -> "geopandas.GeoDataFrame":
    raise TypeError


@functools.singledispatch
def to_dask_geopandas(item, **kwargs) -> "dask_geopandas.GeoDataFrame":
    raise TypeError


# @functools.singledispatch
# def to_pandas(item, **kwargs):
#     raise TypeError


# @functools.singledispatch
# def to_dask_dataframe(item, **kwargs):
#     raise TypeError


# TODO(py3.11): use union type https://github.com/python/cpython/issues/90172
@to_xarray.register(pystac.Item)
@to_xarray.register(pystac.ItemCollection)
def _(
    obj: Union[pystac.Item, pystac.ItemCollection], driver=None, **kwargs
) -> "xarray.Dataset":
    # driver = driver or _infer_driver(item)
    stackstac = _import_optional_dependency(_OptionalDependencies.stackstac)
    return stackstac.stack(obj, **kwargs).to_dataset(name="data")


@to_xarray
def _(obj: pystac.Asset, driver=None, **kwargs) -> "xarray.Dataset":
    # now dispatch on the driver / media type
    xarray = _import_optional_dependency(_OptionalDependencies.xarray)

    if obj.media_type == pystac.MediaType.JSON and "index" in obj.roles:
        planetary_computer = _import_optional_dependency(
            _OptionalDependencies.planetary_computer
        )
        fsspec = _import_optional_dependency(_OptionalDependencies.fsspec)

        r = requests.get(obj.href)
        r.raise_for_status()

        refs = planetary_computer.sign(r.json())
        mapper = fsspec.get_mapper("reference://", fo=refs)
        kwargs = {**{"engine": "zarr", "consolidated": False, "chunks": {}}, **kwargs}
        return xarray.open_dataset(mapper, **kwargs)
    else:
        open_kwargs = obj.extra_fields.get("xarray:open_kwargs", {})
        open_kwargs = {**open_kwargs, **kwargs}
        ds = xarray.open_dataset(obj.href, **open_kwargs)
    return ds


@to_xarray.register
def _(obj: pystac_client.ItemSearch, **kwargs) -> "xarray.Dataset":
    ic = obj.get_all_items()
    return to_xarray(ic, **kwargs)


@to_geopandas.register
def _(obj: pystac.ItemCollection) -> "geopandas.GeoDataFrame":
    geopandas = _import_optional_dependency(_OptionalDependencies.geopandas)
    import shapely
    import pandas

    data = obj.to_dict()["features"]
    geometry = geopandas.array.from_shapely(
        [shapely.geometry.shape(x["geometry"]) for x in data]
    )
    df = geopandas.GeoDataFrame(data, geometry=geometry, crs="epsg:4326")

    properties = pandas.json_normalize(df["properties"])
    assets = pandas.json_normalize(df.assets).rename(columns=lambda x: f"assets.{x}")

    df = pandas.concat(
        [df.drop(columns=["properties", "assets"]), properties, assets], axis="columns"
    )

    return df


@to_dask_geopandas.register
def _(obj: pystac.Asset) -> "dask_geopandas.GeoDataFrame":
    dask_geopandas = _import_optional_dependency(_OptionalDependencies.dask_geopandas)

    storage_options = obj.extra_fields.get("table:storage_options", {})
    return dask_geopandas.read_parquet(obj.href, storage_options=storage_options)
