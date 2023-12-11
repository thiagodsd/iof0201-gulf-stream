"""Project pipelines."""
from typing import Dict

from kedro.pipeline import Pipeline, pipeline

import gulf_stream.pipelines.data_preparation.pipeline as data_preparation

def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.
    """
    data_preparation_pipeline = data_preparation.create_pipeline()
    return {
        "data_preparation": data_preparation_pipeline,
        "__default__": data_preparation_pipeline,
    }
