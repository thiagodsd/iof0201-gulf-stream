"""
This is a boilerplate pipeline 'data_preparation'
generated using Kedro 0.18.2
"""

import wodpy
import pandas as pd
import numpy as np

def osd_extraction(binary_osd):
    """Extracts OSD data from binary file and returns a pandas dataframe"""
    osd = wodpy.profile.ProfileList(binary_osd)
    osd_df = osd.as_dataframe()
    return osd_df