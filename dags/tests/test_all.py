import os
import sys
from io import StringIO
from unittest.mock import patch

import factory
import pandas as pd
import pytest
from faker import Faker

# from ..interview_graphs import load_csv, load_json, map_category_names
from dags.interview_graphs import load_csv, load_json, map_category_names

fake = Faker()

# Add the parent directory to the system path for module imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


# Generate fake video data using Faker library
def generate_video_data():
    """
    Generates fake video data using Faker for testing purposes.

    Returns:
        dict: A dictionary with fake video data including fields like
              video_id, trending_date, title, views, likes, dislikes, etc.
    """
    return {
        "video_id": fake.uuid4(),
        "trending_date": fake.date_this_year(),
        "title": fake.sentence(),
        "channel_title": fake.company(),
        "category_id": fake.random_int(min=1, max=100),
        "publish_time": fake.date_this_decade(),
        "views": fake.random_int(min=1000, max=1000000),
        "likes": fake.random_int(min=100, max=50000),
        "dislikes": fake.random_int(min=0, max=5000),
        "comment_count": fake.random_int(min=10, max=2000),
    }


# Factory for generating fake category data
class JsonCategoryFactory(factory.Factory):
    """
    Factory class for creating fake category data with a random category_id and name.
    Used for testing the mapping of category data.

    Attributes:
        category_id (int): Randomly generated category ID.
        category_name (str): Randomly generated category name.
    """

    class Meta:
        model = dict

    category_id = factory.Faker("random_int", min=1, max=100)
    category_name = factory.Faker("word")


# Fixtures for mocking
@pytest.fixture
def mock_variable_get(mocker):
    """
    Fixture that mocks the `Variable.get` method to simulate fetching the file path
    for the CSV from an Airflow Variable.

    Args:
        mocker: pytest-mock fixture for mocking functions.

    Returns:
        MagicMock: A mocked version of `Variable.get` returning a fake path.
    """
    return mocker.patch(
        "interview_graphs.Variable.get", return_value="/fake/path/to/csv.csv"
    )


@pytest.fixture
def mock_pandas_read_csv(mocker):
    """
    Fixture that mocks `pandas.read_csv` to simulate loading a CSV without actually
    reading from the disk.

    Args:
        mocker: pytest-mock fixture for mocking functions.

    Returns:
        MagicMock: A mocked version of `pandas.read_csv`.
    """
    return mocker.patch("pandas.read_csv")


@pytest.fixture
def mock_airflow_context(mocker):
    """
    Fixture that mocks the Airflow TaskInstance context, specifically the `xcom_push` method.

    Args:
        mocker: pytest-mock fixture for mocking functions.

    Returns:
        dict: A dictionary containing the mocked TaskInstance (ti).
    """
    # Mock the TaskInstance object and its xcom_push method
    mock_task_instance = mocker.MagicMock()
    mock_task_instance.xcom_push = mocker.MagicMock()  # Mock xcom_push method

    # Return a context with the mock TaskInstance
    return {"ti": mock_task_instance}


# Test for load_csv function
def test_load_csv(
    mock_pandas_read_csv, mock_variable_get, mock_airflow_context, mocker
):
    """
    Test the behavior of the load_csv function which reads data from a CSV file,
    processes it, and pushes the result to Airflow's XCom.

    Args:
        mock_pandas_read_csv: Mock of pandas.read_csv to avoid reading from disk.
        mock_variable_get: Mock of Airflow's Variable.get to simulate fetching a file path.
        mock_airflow_context: Mock of Airflow's task instance context.
        mocker: pytest-mock fixture for mocking functions.
    """
    # Create fake video data by generating a list of 10 fake video records
    video_data = [
        generate_video_data() for _ in range(10)
    ]  # Generate 10 fake video records

    # Convert list of dictionaries into a pandas DataFrame for easy processing
    df = pd.DataFrame(video_data)

    # Mock pandas.read_csv to return this DataFrame instead of actually reading from a file
    mock_pandas_read_csv.return_value = df

    # Patch 'open' to simulate reading from a CSV file using StringIO (in-memory string buffer)
    # The CSV is generated from the DataFrame created above
    with patch("builtins.open", return_value=StringIO(df.to_csv(index=False))):
        load_csv(**mock_airflow_context)  # Call the function under test

    # Check if pandas.read_csv was called with the expected path
    mock_pandas_read_csv.assert_called_once_with(
        "/fake/path/to/csv.csv"  # The mock path returned by the mock_variable_get fixture
    )

    # Check if the DataFrame returned from the mock has the correct shape and columns
    assert df.shape == (10, 10)  # Ensure the DataFrame has 10 rows and 10 columns
    assert (
        "video_id" in df.columns
    )  # Ensure 'video_id' column is present in the DataFrame
    assert (
        df["views"].iloc[0] > 1000
    )  # Ensure the 'views' column has valid values (>1000)
    assert (
        df["category_id"].iloc[0] > 0
    )  # Ensure the 'category_id' is a positive integer
    assert df["category_id"].dtype == "int64"  # Ensure 'category_id' is of type int64
    assert (
        df["views"].iloc[0] >= 1000
    )  # Ensure 'views' are within the expected range (>= 1000)

    # Check if the Airflow xcom_push method was called with the processed data
    mock_airflow_context["ti"].xcom_push.assert_called_with(
        key="videos_data",
        value=mocker.ANY,  # Ensure xcom_push was called with any value
    )
