"""
This is a boilerplate pipeline 'data_preparation'
generated using Kedro 0.18.2
"""

from kedro.pipeline import Pipeline, node, pipeline
from .nodes import osd_extraction, lazy_osd_extraction


def create_pipeline(**kwargs) -> Pipeline:
    return Pipeline(
        [
            node(
                func=osd_extraction,
                inputs="params:binary_osd_path",
                outputs="ocean_station_data",
                name="osd_extraction",
            ),
            # kedro run --pipeline data_preparation --node lazy_osd_extraction --params=year.min:1750,year.max:1800
            # kedro run --pipeline data_preparation --node lazy_osd_extraction --params=year.min:1800,year.max:1850
            # kedro run --pipeline data_preparation --node lazy_osd_extraction --params=year.min:1850,year.max:1900
            # kedro run --pipeline data_preparation --node lazy_osd_extraction --params=year.min:1900,year.max:1910
            # kedro run --pipeline data_preparation --node lazy_osd_extraction --params=year.min:1950,year.max:1970
            node(
                func=lazy_osd_extraction,
                inputs=[
                    "params:binary_osd_path",
                    "params:year",
                    "osd_file_positions_input"
                ],
                outputs=[
                    "osd_profiles",
                    "osd_file_positions_output"
                ],
                name="lazy_osd_extraction",
            ),
        ]
    )
