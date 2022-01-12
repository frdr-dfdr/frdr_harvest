from osgeo import gdal

# Need to install GDAL on VM for this to work
    # sudo add-apt-repository ppa:ubuntugis/ppa && sudo apt-get update
    # sudo apt-get update
    # sudo apt-get install gdal-bin
    # sudo apt-get install libgdal-dev
    # export CPLUS_INCLUDE_PATH=/usr/include/gdal
    # export C_INCLUDE_PATH=/usr/include/gdal
    # pip install GDAL


def utm_2_lat_long(file_path):
    dataset = gdal.Open(file_path)
    gdal.Warp("test.tif", dataset, dstSRS="EPSG:3857")
    info2 = gdal.Info("test.tif", format='json')
    west = info2["wgs84Extent"].get("coordinates")[0][0][0]
    east = info2["wgs84Extent"].get("coordinates")[0][2][0]
    north = info2["wgs84Extent"].get("coordinates")[0][0][1]
    south = info2["wgs84Extent"].get("coordinates")[0][2][1]
    # TODO do something with these coordinates

