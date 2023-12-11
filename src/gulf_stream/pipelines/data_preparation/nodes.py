"""
This is a boilerplate pipeline 'data_preparation'
generated using Kedro 0.18.2
"""

import gc
import sys
import warnings

import tqdm
import wodpy

warnings.filterwarnings("ignore")

# import pandas as pd
# import numpy as np


def osd_extraction(binary_osd_path) -> dict:
    """Extracts OSD data from binary file and returns a pandas dataframe"""
    result_dataframe = dict()

    fid = open(binary_osd_path, newline=None)
    profile = wodpy.wod.WodProfile(fid)

    while profile.is_last_profile_in_file(fid) == False:
        print(
            profile.uid(),
            f"({str(profile.year()).zfill(4)}{str(profile.month()).zfill(2)}{str(profile.day()).zfill(2)}) ",
            len(result_dataframe)
        )
        try:
            data = profile.df().copy()

            data["cruise"] = profile.cruise()
            data["day"] = profile.day()
            data["latitude_unc"] = profile.latitude_unc()
            data["longitude_unc"] = profile.longitude_unc()
            data["latitude"] = profile.latitude()
            data["longitude"] = profile.longitude()
            data["month"] = profile.month()
            data["n_levels"] = profile.n_levels()
            # data['primary_header_keys'] = profile.primary_header_keys()
            data["probe_type"] = profile.probe_type()
            data["time"] = profile.time()
            data["uid"] = profile.uid()
            data["year"] = profile.year()
            # data['PIs'] = profile.PIs()
            data["originator_station"] = profile.originator_station()
            data["originator_cruise"] = profile.originator_cruise()
            data["originator_flag_type"] = profile.originator_flag_type()
            # data['extract_secondary_header'] = profile.extract_secondary_header(29)

            result_dataframe[str(profile.uid())] = data.copy()
            del data

            gc.collect()

            profile = wodpy.wod.WodProfile(fid)
        except Exception as e:
            print(e)
            print()
            continue

    return result_dataframe


def lazy_osd_extraction(binary_osd_path, years, file_positions) -> dict:
    """Extracts OSD data from binary file and returns a pandas dataframe"""
    time_key = str(years["min"])
    prev_time_key = sorted(file_positions.keys())[-1]

    profiles = list()
    results = dict()
    results_indexes = dict()
    results_indexes[time_key] = dict()

    f_position = 0
    if prev_time_key in file_positions:
        if "max" in file_positions[prev_time_key]:
            f_position = int(file_positions[prev_time_key]["max"])

    with open(binary_osd_path, newline=None) as fid:
        if f_position > 0:
            fid.seek(f_position)

        profiles.append(wodpy.wod.WodProfile(fid, load_profile_data=False))
        yyyy = profiles[-1].year()
        results_indexes[time_key]["min"] = int(profiles[-1].file_position)

        while (profiles[-1].is_last_profile_in_file(fid) == False) and (yyyy < years["max"]):
            if (yyyy >= int(time_key)):
                profiles.append(wodpy.wod.WodProfile(fid, load_profile_data=False))
                yyyy = profiles[-1].year()
            else:
                p = wodpy.wod.WodProfile(fid, load_profile_data=False)
                yyyy = p.year()
            print(len(profiles), yyyy, end="\r")

    results_indexes[time_key]["max"] = int(profiles[-1].file_position)
    results[time_key] = profiles

    return results, results_indexes
