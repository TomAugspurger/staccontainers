from typing import Union
import functools

import pystac
import pystac_client
import requests
import xarray

from staccontainers._utils import _import_optional_dependency, _OptionalDependencies

    

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
    stackstac = _import_optional_dependency(_OptionalDependencies.stackstac)
    return stackstac.stack(obj, **kwargs).to_dataset(dim="band", promote_attrs=True)


@to_xarray.register
def _(obj: pystac.Asset, driver=None, **kwargs) -> "xarray.Dataset":
    # now dispatch on the driver / media type
    xarray = _import_optional_dependency(_OptionalDependencies.xarray)

    if obj.media_type == pystac.MediaType.JSON and "index" in obj.roles:
        planetary_computer = _import_optional_dependency(
            _OptionalDependencies.planetary_computer
        )
        fsspec = _import_optional_dependency(_OptionalDependencies.fsspec)

        # TODO: fix fix this. Maybe make a pc filesystem that auto-signs.
        # Then use URLs like reference::pc::https://...

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



# le monkeypatches

pystac.Item.to_xarray = to_xarray
pystac.ItemCollection.to_xarray = to_xarray
pystac.Asset.to_xarray = to_xarray
pystac_client.ItemSearch.to_xarray = to_xarray