import datetime
import geopandas
import geopandas.testing
import pandas as pd
import pystac
import pytest
import shapely.geometry
import staccontainers


@pytest.fixture
def item():
    shape = shapely.geometry.box(-1, -1, 1, 1)
    dt = datetime.datetime(2022, 1, 1, tzinfo=datetime.timezone.utc)
    properties = {
        "custom:property": True
    }
    asset = pystac.Asset(href="data.tif", title="My Asset", media_type=pystac.MediaType.COG, roles=["data"])

    item = pystac.Item('id', shapely.geometry.mapping(shape), list(shape.bounds), datetime=dt, properties=properties, assets={"data": asset})
    return item


@pytest.fixture
def item_collection(item):
    ic = pystac.ItemCollection([item], clone_items=True)
    return ic


def test_item_collection_to_geopandas(item_collection):
    shape = shapely.geometry.box(-1, -1, 1, 1)
    result = staccontainers.to_geopandas(item_collection)
    expected = geopandas.GeoDataFrame({
        "type": ["Feature"],
        "stac_version": ["1.0.0"],
        "id": ["id"],
        "geometry": geopandas.array.from_shapely([shape]),
        "links": [[]],
        "bbox": [list(shape.bounds)],
        "stac_extensions": [[]],
        "custom:property": [True],
        "datetime": pd.to_datetime(["2022-01-01T00:00:00Z"]),
        "assets.data.href": ["data.tif"],
        "assets.data.type": [pystac.MediaType.COG.value],
        "assets.data.title": ["My Asset"],
        "assets.data.roles": [["data"]]
    }, crs="epsg:4326")
    geopandas.testing.assert_geodataframe_equal(result, expected)


@pytest.mark.parametrize("media_type", [
    "application/x-parquet",
    "x-gis/x-shapefile",
])
def test_asset_to_geopandas(tmp_path, media_type):
    file = tmp_path / "data"
    geometry = geopandas.array.from_shapely([
        shapely.geometry.box(-1, -1, 1, 1),
        shapely.geometry.box(-2, -2, 1, 1),
    ])
    df = geopandas.GeoDataFrame({"a": [1, 2], "geometry": geometry}, crs="WGS84")
    if media_type == "application/x-parquet":
        df.to_parquet(file)
    else:
        df.to_file(file)

    asset = pystac.Asset(str(file), title="My Asset", media_type=media_type)
    result = staccontainers.to_geopandas(asset)
    geopandas.testing.assert_geodataframe_equal(result, df)