import pystac_client
import planetary_computer

from staccontainers import *


def test_asset_raster():
    catalog = pystac_client.Client.open("https://planetarycomputer.microsoft.com/api/stac/v1", sign_function=planetary_computer.sign)
    (
        catalog
            .get_collection("sentinel-2-l2a")
            .get_item("S2B_MSIL2A_20220612T182919_R027_T24XWR_20220613T123251")
            .assets["B03"]
    ).to_xarray()