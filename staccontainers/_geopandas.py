import functools

import pystac
import geopandas
import shapely
import pandas


@functools.singledispatch
def to_geopandas(item, **kwargs) -> "geopandas.GeoDataFrame":
    raise TypeError



@to_geopandas.register
def _(obj: pystac.ItemCollection) -> "geopandas.GeoDataFrame":
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


@to_geopandas.register
def _(obj: pystac.Asset) -> "geopandas.GeoDataFrame":
    storage_options = obj.extra_fields.get("table:storage_options", {})
    # TODO: media-type based dispatch.
    return geopandas.read_parquet(obj.href, storage_options=storage_options)



pystac.Asset.to_geopandas = to_geopandas
pystac.ItemCollection.to_geopandas = to_geopandas