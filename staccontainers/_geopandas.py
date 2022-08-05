import functools
from multiprocessing.sharedctypes import Value

import pystac
import pystac_client
import geopandas
import shapely
import pandas as pd


@functools.singledispatch
def to_geopandas(obj, **kwargs) -> "geopandas.GeoDataFrame":
    """
    Convert to a :class:`geopandas.GeoDataFrame`.
    
    Parameters
    ----------
    obj: the object to convert

        This method supports converting the following types

        * :class:`pystac.ItemCollection` or :class:`pystac_client.ItemSearch`
          by converting each item to a row in the output ``GeoDataFrame``.
        * :class:`pystac.Asset` by loading the asset. The
          asset will use the asset's media type to determine the appropriate
          method to use to load the data.

    Returns
    -------
    geopandas.GeoDataFrame

    Examples
    --------


    """
    raise TypeError



@to_geopandas.register
def item_collection_to_geopandas_geodataframe(obj: pystac.ItemCollection) -> "geopandas.GeoDataFrame":
    data = obj.to_dict()["features"]
    geometry = geopandas.array.from_shapely(
        [shapely.geometry.shape(x["geometry"]) for x in data]
    )
    df = geopandas.GeoDataFrame(data, geometry=geometry, crs="epsg:4326")

    properties = pd.json_normalize(df["properties"])
    assets = pd.json_normalize(df["assets"]).rename(columns=lambda x: f"assets.{x}")

    df = pd.concat(
        [df.drop(columns=["properties", "assets"]), properties, assets], axis="columns"
    )
    for name in ["datetime", "start_datetime", "end_datetime"]:
        if name in df:
            df[name] = pd.to_datetime(df[name])


    return df


@to_geopandas.register
def asset_to_geopandas_geodataframe(obj: pystac.Asset, reader=None, **kwargs) -> "geopandas.GeoDataFrame":
    """
    Convert a :class:`pystac.Asset` to a :class:`geopandas.GeoDataFrame`.

    This function reads the data in the asset's ``href`` and returns it as a GeoDataFrame.

    Parameters
    ----------
    obj: pystac Asset
    reader: callable, optional
    **kwargs

    """
    storage_options = obj.extra_fields.get("table:storage_options", {})
    if storage_options:
        kwargs.setdefault("storage_options", storage_options)
    if reader is None:
        if obj.media_type == "application/x-parquet":
            reader = geopandas.read_parquet
        else:
            reader = geopandas.read_file
        # else:
        #     raise ValueError(f"Unrecognized media type '{obj.media_type}'. Please provide a 'reader'")

    return reader(obj.href, **kwargs)


@to_geopandas.register
def item_search_to_geopandas_geodataframe(obj: pystac_client.ItemSearch, **kwargs) -> "geopandas.GeoDataFrame":
    ic = obj.get_all_items()
    return to_geopandas(ic, **kwargs)


def _patch():
    pystac.Asset.to_geopandas = to_geopandas
    pystac.ItemCollection.to_geopandas = to_geopandas
    pystac_client.ItemSearch.to_geopandas = to_geopandas