import functools
import pystac

import dask_geopandas


@functools.singledispatch
def to_dask_geopandas(item, **kwargs) -> "dask_geopandas.GeoDataFrame":
    raise TypeError



@to_dask_geopandas.register
def _(obj: pystac.Asset) -> "dask_geopandas.GeoDataFrame":
    storage_options = obj.extra_fields.get("table:storage_options", {})
    # TODO: media-type based dispatch.
    return dask_geopandas.read_parquet(obj.href, storage_options=storage_options)


pystac.Asset.to_dask_geopandas = to_dask_geopandas