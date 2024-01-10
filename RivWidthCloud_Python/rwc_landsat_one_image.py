import argparse
import ee
import getopt
import numpy as np
import sys

from typing import Union
from functools import partial

# local imports
from functions_landsat import id2Img
from rwc_landsat import rwGenSR


def _cli() -> dict:
    """Cli for completing single image river width analysis"""
    parser = argparse.ArgumentParser(
        prog="rwc_landsat_one_image.py",
        description="Calculate river centerline and width in the provided Landsat scene. \
    (Example: python rwc_landsat_one_image.py LC08_L1TP_022034_20130422_20170310_01_T1 -f shp)",
    )

    parser.add_argument(
        "landsat_id", help="LANDSAT_ID for any Landsat 5, 7, and 8 SR scene", type=str
    )
    parser.add_argument(
        "-f",
        "--file_format",
        help="Output file format ('csv' or 'shp'). Default: 'csv'",
        type=str,
        default="csv",
    )
    parser.add_argument(
        "-w",
        "--water_method",
        help="Water classification method ('Jones2019' or 'Zou2018'). Default: 'Jones2019'",
        type=str,
        default="Jones2019",
    )
    parser.add_argument(
        "-d", "--max_distance", help="Default: 4000 meters", type=float, default=4000
    )
    parser.add_argument(
        "-i", "--fill_size", help="Default: 333 pixels", type=float, default=333
    )
    parser.add_argument(
        "-b",
        "--max_distance_branch_removal",
        help="Default: 500 pixels",
        type=float,
        default=500,
    )
    parser.add_argument(
        "-o",
        "--output_folder",
        help="Any existing folder name in Google Drive. Default: root of Google Drive",
        type=str,
        default="",
    )

    group_validation = parser.add_argument_group(
        title="Run the RivWidthCloud in point mode",
        description="In point mode, width only calculated for the region close to the point \
    location specified by its lon, lat, and an identifier. The radius of the region is specified through the specified buffer. \
    The point must locate within the bounds of the scene. \
    (Example: python rwc_landsat_one_image.py LC08_L1TP_022034_20130422_20170310_01_T1 -f shp -w Zou2018 -p -x -88.263 -y 37.453 -r 2000 -n testPoint)",
    )

    group_validation.add_argument(
        "-p", "--point_mode", help="Enable the point mode", action="store_true"
    )
    group_validation.add_argument(
        "-x", "--lon", help="Longitude of the point location", type=float
    )
    group_validation.add_argument(
        "-y", "--lat", help="Latitude of the point location", type=float
    )
    group_validation.add_argument(
        "-r",
        "--radius",
        help="Radius of the buffered region around the point location",
        type=float,
        default=4000,
    )

    return vars(parser.parse_args())

    # IMG_ID = args.LANDSAT_ID
    # FORMAT = args.FORMAT
    # WATER_METHOD = args.WATER_METHOD
    # MAXDISTANCE = args.MAXDISTANCE
    # FILL_SIZE = args.FILL_SIZE
    # MAXDISTANCE_BRANCH_REMOVAL = args.MAXDISTANCE_BRANCH_REMOVAL
    # OUTPUT_FOLDER = args.OUTPUT_FOLDER

    # POINTMODE = args.POINT
    # LONGITUDE = args.LONGITUDE
    # LATITUDE = args.LATITUDE
    # RADIUS = args.BUFFER
    # ROI_NAME = args.POINT_NAME


class Rwc:
    def __init__(self, img_id):
        ee.Initialize()

        self.img_id = img_id

    @staticmethod
    def one_image_river_width(
        landsat_id: str = None,
        lon: Union[float, None] = None,
        lat: Union[float, None] = None,
        max_distance: float = 4000,
        max_distance_branch_removal: float = 500,
        fill_size: float = 333,
        file_format: str = "csv",
        output_folder: Union[str, None] = None,
        point_mode: bool = False,
        radius: float = 4000,
        water_method: str = "Jones2019",
    ):
        try:
            # Get landsat image from landsat id
            _img = id2Img(landsat_id)

        except ee.ee_exception.EEException as e:
            ee.Initialize()

        _img = id2Img(landsat_id)

        aoi = (
            (ee.Geometry.Point([lon, lat], "EPSG:4326").buffer(radius).bounds())
            if point_mode
            else None
        )

        export_prefix = landsat_id + "_v_" if aoi else landsat_id

        rwc = rwGenSR(
            aoi=aoi,
            WATER_METHOD=water_method,
            MAXDISTANCE=max_distance,
            FILL_SIZE=fill_size,
            MAXDISTANCE_BRANCH_REMOVAL=max_distance_branch_removal,
        )

        widthOut = rwc(_img)
        # print(widthOut.toDictionary())
        # print(
        #     [x for x in dir(widthOut)]
        # )

        taskWidth = ee.batch.Export.table.toDrive(
            collection=widthOut,
            description=export_prefix,
            folder=output_folder,
            fileNamePrefix=export_prefix,
            fileFormat=file_format,
        )
        taskWidth.start()

        print(
            f"{export_prefix} will be exported to {output_folder} as {file_format} file"
        )


if __name__ == "__main__":
    kwargs = _cli()
    rwc = Rwc.one_image_river_width(**kwargs)
    # rwc = Rwc()
