from osgeo import gdal


def utm_2_lat_long(file_path):
    dataset = gdal.Open(file_path)
    gdal.Warp("test.tif", dataset, dstSRS="EPSG:3857")
    info2 = gdal.Info("test.tif", format='json')
    west = info2["wgs84Extent"].get("coordinates")[0][0][0]
    east = info2["wgs84Extent"].get("coordinates")[0][2][0]
    north = info2["wgs84Extent"].get("coordinates")[0][0][1]
    south = info2["wgs84Extent"].get("coordinates")[0][2][1]
    # TODO do something with these coordinates

