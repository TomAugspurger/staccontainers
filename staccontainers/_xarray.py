from typing import Union
import functools

import pystac
import pystac_client
import requests
import xarray

# from staccontainers._utils import _import_optional_dependency, _OptionalDependencies

    

@functools.singledispatch
def to_xarray(item, **kwargs) -> xarray.Dataset:
    raise TypeError



# TODO(py3.11): use union type https://github.com/python/cpython/issues/90172
@to_xarray.register(pystac.Item)
@to_xarray.register(pystac.ItemCollection)
def _(
    obj: Union[pystac.Item, pystac.ItemCollection], driver=None, **kwargs
) -> "xarray.Dataset":
    # driver = driver or _infer_driver(item)
    import stackstac
    # stackstac = _import_optional_dependency(_OptionalDependencies.stackstac)
    return stackstac.stack(obj, **kwargs).to_dataset(dim="band", promote_attrs=True)


@to_xarray.register
def _(obj: pystac.Asset, driver=None, **kwargs) -> "xarray.Dataset":
    # now dispatch on the driver / media type
    import planetary_computer
    import fsspec

    if obj.media_type == pystac.MediaType.JSON and "index" in obj.roles:
        r = requests.get(obj.href)
        r.raise_for_status()

        refs = planetary_computer.sign(r.json())
        mapper = fsspec.get_mapper("reference://", fo=refs)
        kwargs = {**{"engine": "zarr", "consolidated": False, "chunks": {}}, **kwargs}
        return xarray.open_dataset(mapper, **kwargs)
    elif obj.media_type == pystac.MediaType.COG:
        open_kwargs = obj.extra_fields.get("xarray:open_kwargs", {})
        open_kwargs = {**open_kwargs, **kwargs}
        open_kwargs.setdefault("engine", "rasterio")
        ds = xarray.open_dataset(obj.href, **open_kwargs)
    elif obj.media_type == "application/vnd+zarr":
        return _open_asset_zarr(obj)
    else:
        open_kwargs = obj.extra_fields.get("xarray:open_kwargs", {})
        open_kwargs = {**open_kwargs, **kwargs}
        ds = xarray.open_dataset(obj.href, **open_kwargs)

    return ds


def _open_asset_zarr(asset: pystac.Asset, **kwargs) -> xarray.Dataset:
    kwargs = {**kwargs, **asset.extra_fields.get("xarray:open_kwargs", {})}
    kwargs.setdefault("engine", "zarr")

    return xarray.open_dataset(asset.href, **kwargs)


@to_xarray.register
def _(obj: pystac_client.ItemSearch, **kwargs) -> "xarray.Dataset":
    ic = obj.get_all_items()
    return to_xarray(ic, **kwargs)


def _patch():
    pystac.Item.to_xarray = to_xarray
    pystac.ItemCollection.to_xarray = to_xarray
    pystac.Asset.to_xarray = to_xarray
    pystac_client.ItemSearch.to_xarray = to_xarray