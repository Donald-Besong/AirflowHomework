import json
import logging
import os
from typing import Any, Dict, List, cast

import pandas as pd
import pendulum
from tabulate import tabulate

from airflow import DAG
from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import PythonOperator
from dags.utils.graphing import plot_top_categories_by_views, plot_top_channels

save_dir = os.path.expanduser("~/airflow/plots/")
full_save_path = os.path.join(save_dir, "enriched_videos.csv")


def load_csv(**context: Any) -> None:
    """
    Load a CSV file into a Pandas DataFrame and push it to XCom as a JSON string.
    Note that I did not use dtypes and pase_dates becasue I am in a hurry

    This function is intended to be used as an Airflow PythonOperator task.

    Args:
        **context (Any): Airflow context dictionary containing runtime info.
                         The key 'ti' contains the TaskInstance, used for XCom.

    Returns:
        None

    Raises:
        FileNotFoundError: If the CSV file does not exist at the specified path.
        pd.errors.ParserError: If there is an error parsing the CSV.

    Usage in Airflow:
        This function should be set as the `python_callable` for a PythonOperator.
        It uses `ti.xcom_push()` to push the JSON string representation of the DataFrame
        to XCom with the key "videos_data".

    Typing notes:
        - `context` is typed as `Any` because Airflow passes a dynamic context dict.
        - We use `cast(TaskInstance, context["ti"])` to inform mypy that `ti` is a TaskInstance,
          enabling static type checking for `.xcom_push()`.

    Data cleansing/reduction:
         - The dataset initial had the following columns:
         ['video_id', 'trending_date', 'title', 'channel_title', 'category_id',
       'publish_time', 'tags', 'views', 'likes', 'dislikes', 'comment_count',
       'thumbnail_link', 'comments_disabled', 'ratings_disabled',
       'video_error_or_removed', 'description'].
       - I removed  ['thumbnail_link', 'comments_disabled', 'ratings_disabled',
       'video_error_or_removed', 'description']
    """
    csv_path = Variable.get(
        "csv_path",
        default_var=os.path.expanduser(
            "~/Documents/Tutorials/ResponseMediaTest/GBvideos.csv"
        ),
    )
    df: pd.DataFrame = pd.read_csv(csv_path)
    relevant_columns = [
        "video_id",
        "trending_date",
        "title",
        "channel_title",
        "category_id",
        "publish_time",
        "views",
        "likes",
        "dislikes",
        "comment_count",
    ]
    df = df[relevant_columns]
    table: str = tabulate(
        df.head().to_dict(orient="records"), headers="keys", tablefmt="grid"
    )
    logging.info("\n%s", table)
    logging.info("\n ----columns---")
    logging.info("\n%s", df.columns)

    ti = cast(TaskInstance, context["ti"])
    ti.xcom_push(key="videos_data", value=df.to_json(orient="records"))


def load_json(**context: Any) -> None:
    """
    Load a JSON file containing category mappings and push to XCom.

    Args:
        **context (Any): Airflow context dictionary. The 'ti' key is TaskInstance.

    Returns:
        None

    Raises:
        FileNotFoundError: If the JSON file does not exist.
        json.JSONDecodeError: If the JSON content is invalid.

    Usage in Airflow:
        Used as a PythonOperator callable that loads a category ID to name map,
        then pushes it to XCom with key "category_map".

    Usage of the Variable class:
        Variable.get("csv_path", default_var=...) returns the default value if
        the variable "csv_path" does not exist in Airflow’s metadata database.
        It does not create or save the variable in Airflow’s Variables store automatically.
        That was the case for load_csv() above. Airflow will try to use the variable from metadata,
        and not finding it, it will resort to the default value in the code.
        For demonstration purposes, load_json will apply a more dynamic creation of
        the variable.
        Go to Admin > Variables, Click "+" (Add variable), Set Key: json_path
        Set Value: your desired CSV path (/home/donald/Documents/Tutorials/ResponseMediaTest/GB_category_id.json").
        The variable gets added in metadata, and Airflow will first look for it there. If absent, it will
        use the default value in the code.

    Data cleansing/reduction:
        - The json fine is filtered into a new dictionary that only had the category_id as the and category_name as value.

    Typing notes:
        Similar to `load_csv`, context is dynamically typed and cast to TaskInstance.
    """
    json_path = Variable.get(
        "json_path",
        default_var=os.path.expanduser(
            "~/Documents/Tutorials/ResponseMediaTest/GB_category_id.json"
        ),
    )
    with open(json_path) as f:
        data: Dict[str, str] = json.load(f)  # Expecting int keys to str values

    items = cast(List[Dict[str, Any]], data.get("items", []))

    # category_map = {
    #     item["id"]: item["snippet"]["title"] for item in data.get("items", [])
    # }
    category_map = {item["id"]: item["snippet"]["title"] for item in items}

    table: str = tabulate(
        category_map.items(), headers=["category_id", "category_name"], tablefmt="grid"
    )
    logging.info("\n%s", table)

    ti = cast(TaskInstance, context["ti"])
    ti.xcom_push(key="category_map", value=category_map)


def map_category_names(**context: Any) -> None:
    """
    Retrieve videos data and category map from XCom, map category IDs to names,
    and log a tabulated preview of the enriched DataFrame.

    Args:
        **context (Any): Airflow runtime context with TaskInstance under 'ti'.

    Returns:
        None

    Raises:
        KeyError: If XCom keys "videos_data" or "category_map" are missing.
        ValueError: If JSON parsing fails.

    Usage in Airflow:
        This function pulls JSON strings from upstream tasks via XCom,
        converts the video data back into a DataFrame, maps category IDs to names,
        then logs the first few rows in a nicely formatted table using `tabulate`.

    Typing notes:
        Uses `cast` to specify that `ti` is TaskInstance for XCom access.
        Explicit type annotations on variables for static analysis.

    Saving the resulting data:
        Persist the enriched YouTube videos DataFrame with category names to a local CSV file.
        This task retrieves the videos data and category map from XCom pushed by upstream tasks,
        reconstructs the DataFrame, maps category IDs to their human-readable names,
        and saves the resulting enriched dataset to a local file path specified by an Airflow Variable.
        The save path can be configured dynamically in the Airflow UI under Admin → Variables
        by setting the variable 'enriched_csv_path'. If unset, it defaults to default_save_dir .
    """

    ti = cast(TaskInstance, context["ti"])
    videos_json: str = ti.xcom_pull(key="videos_data", task_ids="load_csv")
    category_map: Dict[str, str] = ti.xcom_pull(
        key="category_map", task_ids="load_json"
    )
    category_map_int = {int(k): v for k, v in category_map.items() if k.isdigit()}

    df: pd.DataFrame = pd.read_json(videos_json, orient="records")
    df["category_name"] = df["category_id"].map(category_map_int)

    save_path = Variable.get("enriched_csv_path", default_var=full_save_path)

    # Ensure directory exists
    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    df.to_csv(save_path, index=False)

    logging.info(f"Saved enriched video data to {save_path}")

    ti.xcom_push(key="saved_csv_path", value=save_path)

    table: str = tabulate(
        df.head().to_dict(orient="records"), headers="keys", tablefmt="grid"
    )
    logging.info("\n%s", table)


def load_from_local(**context: Any) -> None:
    """
    Load the locally saved enriched YouTube videos CSV file and log a preview.

    This task pulls the saved file path from XCom pushed by `save_to_local`.
    If the path is missing, it falls back to the Airflow Variable 'enriched_csv_path',
    allowing flexible configuration via the Airflow UI.

    It reads the CSV into a Pandas DataFrame and logs the first few rows using
    a well-formatted table for easy inspection.

    Args:
        **context (Any): Airflow runtime context dictionary, expects 'ti' (TaskInstance).

    Raises:
        FileNotFoundError: If the file does not exist at the specified path.

    Usage:
        Suitable for debugging or verification tasks after data persistence.

    Notes:
        - Uses `tabulate` for human-readable logging output.
        - Provides graceful fallback mechanisms for missing XCom keys.
    """
    ti = cast(TaskInstance, context["ti"])

    saved_path: str = ti.xcom_pull(key="saved_csv_path", task_ids="save_to_local")

    # Fallback if XCom is missing
    if not saved_path:
        saved_path = Variable.get("enriched_csv_path", default_var=full_save_path)

    if not saved_path or not os.path.exists(saved_path):
        logging.error(f"Enriched CSV file not found at: {saved_path}")
        return

    df = pd.read_csv(saved_path)
    plot_top_categories_by_views(df, save_dir)
    plot_top_channels(df, save_dir)
    table: str = tabulate(
        df.head().to_dict(orient="records"), headers="keys", tablefmt="grid"
    )
    logging.info(f"Loaded enriched video data preview from {saved_path}:\n{table}")


with DAG(
    dag_id="interviewgraphs_process_youtube_data",
    start_date=pendulum.today("UTC").add(days=-1),
    schedule=None,
    catchup=False,
) as dag:
    """
    DAG to process YouTube video data by loading CSV and JSON files,
    enriching video data with category names, and logging a preview.

    Schedule:
        Manually triggered only (schedule_interval=None).

    Tasks:
        - load_csv: Loads CSV video data into XCom.
        - load_json: Loads category mapping JSON into XCom.
        - map_category_names: Maps category IDs to names and logs results.
    """

    task_load_csv = PythonOperator(
        task_id="load_csv",
        python_callable=load_csv,
    )

    task_load_json = PythonOperator(
        task_id="load_json",
        python_callable=load_json,
    )

    task_map_categories = PythonOperator(
        task_id="map_category_names",
        python_callable=map_category_names,
    )

    task_load_from_local = PythonOperator(
        task_id="load_from_local",
        python_callable=load_from_local,
    )

    [task_load_csv, task_load_json] >> task_map_categories >> task_load_from_local
