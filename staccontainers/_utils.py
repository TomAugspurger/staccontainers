import enum
import importlib


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

