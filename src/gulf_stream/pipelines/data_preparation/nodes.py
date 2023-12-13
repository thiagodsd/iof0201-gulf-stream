"""
This is a boilerplate pipeline 'data_preparation'
generated using Kedro 0.18.2
"""

import gc
import sys
import warnings

import tqdm
import wodpy

import pandas as pd
import numpy as np

import multiprocessing as mp

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


def lazy_osd_to_dataframe(p):
    result_dataframe = pd.DataFrame()
    try:
        data = p.df().copy()

        data["cruise"] = p.cruise()
        data["day"] = p.day()
        data["latitude_unc"] = p.latitude_unc()
        data["longitude_unc"] = p.longitude_unc()
        data["latitude"] = p.latitude()
        data["longitude"] = p.longitude()
        data["month"] = p.month()
        data["n_levels"] = p.n_levels()
        # data['primary_header_keys'] = p.primary_header_keys()
        data["probe_type"] = p.probe_type()
        data["time"] = p.time()
        data["uid"] = p.uid()
        data["year"] = p.year()
        # data['PIs'] = p.PIs()
        data["originator_station"] = p.originator_station()
        data["originator_cruise"] = p.originator_cruise()
        data["originator_flag_type"] = p.originator_flag_type()
        # data['extract_secondary_header'] = p.extract_secondary_header(29)

        result_dataframe = pd.concat([result_dataframe, data], ignore_index=True, axis=0)
        del data

        gc.collect()
    except Exception as _:
        pass

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

        profiles.append(wodpy.wod.WodProfile(fid, load_profile_data=True))
        yyyy = profiles[-1].year()
        results_indexes[time_key]["min"] = int(profiles[-1].file_position)

        while (profiles[-1].is_last_profile_in_file(fid) == False) and (yyyy < years["max"]):
            if (yyyy >= int(time_key)):
                profiles.append(wodpy.wod.WodProfile(fid, load_profile_data=True))
                yyyy = profiles[-1].year()
            else:
                p = wodpy.wod.WodProfile(fid, load_profile_data=True)
                yyyy = p.year()
            print(len(profiles), yyyy, end="\r")

    results_indexes[time_key]["max"] = int(profiles[-1].file_position)
    results[time_key] = profiles

    print(f"Extracted {len(profiles)} profiles from {binary_osd_path} between {time_key} and {years['max']}")

    # using multiprocessing to concatenate data with lazy_osd_to_dataframe
    pool = mp.Pool(np.min([mp.cpu_count()-1, len(profiles)]))
    result = pool.map(lazy_osd_to_dataframe, results[time_key])
    result = pd.concat(result, ignore_index=True, axis=0)
    pool.close()
    pool.join()

    return {time_key:result}, results_indexes


# def lazy_osd_to_dataframe(osd_profiles, year):
#     dataframe_list = pd.DataFrame()
#     result_dataframe = dict()
    
#     data_list = osd_profiles[str(year)]()
#     print(data_list)
#     for p in tqdm.tqdm(data_list):
#         try:
#             data = p.df().copy()

#             data["cruise"] = p.cruise()
#             data["day"] = p.day()
#             data["latitude_unc"] = p.latitude_unc()
#             data["longitude_unc"] = p.longitude_unc()
#             data["latitude"] = p.latitude()
#             data["longitude"] = p.longitude()
#             data["month"] = p.month()
#             data["n_levels"] = p.n_levels()
#             # data['primary_header_keys'] = p.primary_header_keys()
#             data["probe_type"] = p.probe_type()
#             data["time"] = p.time()
#             data["uid"] = p.uid()
#             data["year"] = p.year()
#             # data['PIs'] = p.PIs()
#             data["originator_station"] = p.originator_station()
#             data["originator_cruise"] = p.originator_cruise()
#             data["originator_flag_type"] = p.originator_flag_type()
#             # data['extract_secondary_header'] = p.extract_secondary_header(29)

#             dataframe_list = pd.concat([dataframe_list, data], ignore_index=True, axis=0)
#             del data

#             gc.collect()
#         except Exception as _:
#             continue

#     print(dataframe_list)

#     if len(dataframe_list) > 0:
#         result_dataframe[str(year)] = dataframe_list#pd.concat(dataframe_list, ignore_index=True, axis=0)
#     else:
#         result_dataframe[str(year)] = pd.DataFrame()

#     return result_dataframe