import ee
import numpy as np
import pandas as pd
import getopt
import os
from os import listdir
import pathlib
import argparse
from functools import partial
from typing import Union

from functions_batch import maximum_no_of_tasks
from rwc_landsat_one_image import Rwc

from multiprocessing import Pool
import concurrent.futures as cf
import asyncio


def _cli() -> dict:
    parser = argparse.ArgumentParser(
        prog="rwc_landsat_batch.py",
        description="Batch execute rwc_landsat.py for a csv files that contains Landsat image IDs and/or point locations.\
    (Example: python rwc_landsat_batch.py example_batch_input/example_batch_input.csv)",
    )
    parser.add_argument(
        "landsat_id_file",
        help='Csv file contains at least one column (named "landsat_id")',
        type=pathlib.Path,
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
        help="default: 500 pixels",
        type=float,
        default=500,
    )
    parser.add_argument(
        "-o",
        "--output_folder",
        help="any existing folder name in google drive. default: root of google Drive",
        type=str,
        default="",
    )
    parser.add_argument(
        "-m",
        "--maximum_number_of_tasks",
        help="Maximum number of tasks running simutaneously on the server. Default: 10",
        type=int,
        default=10,
    )
    parser.add_argument(
        "-s",
        "--start_number",
        help="(Re)starting task No. Helpful when restarting an interrupted batch processing. Default: 0 (start from the beginning)",
        type=int,
        default=0,
    )

    group_validation = parser.add_argument_group(
        title="Batch run the RivWidthCloud in POINT mode",
        description='In POINT mode, the csv file needs to have addtional columns named "Point_ID", "Longitude", and "Latitude"\
    The point must locate within the bounds of the scene. \
    (Example: python rwc_landsat_batch.py example_batch_input/example_batch_input.csv -p)',
    )

    group_validation.add_argument(
        "-p", "--point_mode", help="Enable the POINT mode", action="store_true"
    )
    group_validation.add_argument(
        "-r",
        "--radius",
        help="Radius of the buffered region around the point location",
        type=float,
        default=4000,
    )

    return vars(parser.parse_args())


def _caller(*args, func=None) -> partial:
    if len(args) > 1:
        landsat_id, lat, lon = args
        return partial(func, landsat_id=landsat_id, lat=lat, lon=lon)()
    else:
        return partial(func, landsat_id=args[0])()
    # p()
    # print(f"runnning on {os.pid()}")


def rwc_landsat_batch(
    landsat_id_file: str = None,
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
    start_number: int = 0,
    maximum_number_of_tasks: int = 10,
):
    output_folder = "gee"

    _landsat_one_image_partial = partial(
        Rwc.one_image_river_width,
        max_distance=max_distance,
        max_distance_branch_removal=max_distance_branch_removal,
        fill_size=fill_size,
        file_format=file_format,
        output_folder=output_folder,
        point_mode=point_mode,
        radius=radius,
        water_method=water_method,
    )

    if point_mode:
        imageInfo = pd.read_csv(
            landsat_id_file, dtype={"Point_ID": np.unicode_, "LANDSAT_ID": np.unicode_}
        )
        point_IDList = list(imageInfo["Point_ID"].values)
        x = list(imageInfo["Longitude"].values)
        y = list(imageInfo["Latitude"].values)

        return _landsat_one_image_partial, zip(point_IDList, x, y)

    else:
        imageInfo = pd.read_csv(landsat_id_file, dtype={"LANDSAT_ID": np.unicode_})

        sceneIDList = list(imageInfo["LANDSAT_ID"].values)

        return _landsat_one_image_partial, zip(sceneIDList)


if __name__ == "__main__":
    kwargs = _cli()

    partial_func, iter_object = rwc_landsat_batch(**kwargs)

    with Pool(5) as pool:
        task = pool.starmap(partial(_caller, func=partial_func), iter_object)
        print(task)
