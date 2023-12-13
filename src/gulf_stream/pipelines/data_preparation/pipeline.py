"""
This is a boilerplate pipeline 'data_preparation'
generated using Kedro 0.18.2
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import (
    # osd_extraction, 
    lazy_osd_extraction,
    lazy_osd_to_dataframe
)


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            # node(
            #     func=osd_extraction,
            #     inputs="params:binary_osd_path",
            #     outputs="ocean_station_data",
            #     name="osd_extraction",
            # ),
            # kedro run --pipeline data_preparation --node lazy_osd_extraction --params=year_range.min:1700,year_range.max:1800
            # kedro run --pipeline data_preparation --node lazy_osd_extraction --params=year_range.min:1700,year_range.max:1900
            # kedro run --pipeline data_preparation --node lazy_osd_extraction --params=year_range.min:1900,year_range.max:1950
            # kedro run --pipeline data_preparation --node lazy_osd_extraction --params=year_range.min:1950,year_range.max:1960
            # kedro run --pipeline data_preparation --node lazy_osd_extraction --params=year_range.min:1960,year_range.max:1970
            # kedro run --pipeline data_preparation --node lazy_osd_extraction --params=year_range.min:1970,year_range.max:1980
            # kedro run --pipeline data_preparation --node lazy_osd_extraction --params=year_range.min:1980,year_range.max:1990
            # kedro run --pipeline data_preparation --node lazy_osd_extraction --params=year_range.min:1990,year_range.max:2000
            # kedro run --pipeline data_preparation --node lazy_osd_extraction --params=year_range.min:2000,year_range.max:2010
            # kedro run --pipeline data_preparation --node lazy_osd_extraction --params=year_range.min:2010,year_range.max:2020
            # kedro run --pipeline data_preparation --node lazy_osd_extraction --params=year_range.min:2020,year_range.max:2030
            node(
                func=lazy_osd_extraction,
                inputs=[
                    "params:binary_osd_path",
                    "params:year_range",
                    "osd_file_positions_input"
                ],
                outputs=[
                    "ocean_station_data",
                    "osd_file_positions_output"
                ],
                name="lazy_osd_extraction",
            ),
            # kedro run --pipeline data_preparation --node lazy_osd_to_dataframe --params=year:1700
            # node(
            #     func=lazy_osd_to_dataframe,
            #     inputs=[
            #         "osd_profiles",
            #         "params:year",
            #     ],
            #     outputs="ocean_station_data",
            #     name="lazy_osd_to_dataframe",
            # ),
        ]
    )
