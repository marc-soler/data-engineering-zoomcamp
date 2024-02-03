import re
import pandas as pd

if "transformer" not in globals():
    from mage_ai.data_preparation.decorators import transformer
if "test" not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    # Eliminating rows with passenger count or trip distance == 0
    data = data[
        (data["passenger_count"] > 0) & (data["trip_distance"] > 0)
    ].reset_index(drop=True)
    # lpep_pickup_datetime to date
    data["lpep_pickup_date"] = data["lpep_pickup_datetime"].dt.strftime("%Y-%m-%d")
    # Column names from camel case to snake case
    data = data.rename(
        columns={
            "VendorID": "vendor_id",
            "RatecodeID": "ratecode_id",
            "PULocationID": "pu_location_id",
            "DOLocationID": "do_location_id",
        }
    )

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert "vendor_id" in output.columns
    assert output["passenger_count"].isin([0]).sum() == 0
    assert output["trip_distance"].isin([0]).sum() == 0
