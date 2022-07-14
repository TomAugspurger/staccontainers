"""
staccontainers: Convert STAC objects to data containers.

This library makes it easy to go from STAC objects (Items, Assets, ItemCollections, etc.)
to data containers (xarray.Dataset, geopandas.GeoDataFrame, etc.)
"""
__version__ = "0.1.0"
__all__ = []


try:
    from staccontainers._dask_geopandas import to_dask_geopandas
except ImportError:
    pass
else:
    __all__.append("to_dask_geopandas")

try:
    from staccontainers._geopandas import to_geopandas
except ImportError:
    pass
else:
    __all__.append("to_geopandas")

try:
    from staccontainers._xarray import to_xarray
except ImportError:
    pass
else:
    __all__.append("to_xarray")