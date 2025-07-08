import os

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def plot_top_categories_by_views(
    df: pd.DataFrame, save_dir: str, output_path: str = "top_categories_views.png"
) -> None:
    """
    Generate a bar chart of top content categories by total views.

    Args:
        df (pd.DataFrame): dataframe from **context["it"]
        output_path (str): Path to save the output plot image.
    """
    output_path = os.path.join(save_dir, output_path)
    if "category_name" not in df.columns or "views" not in df.columns:
        raise ValueError("Required columns missing from the DataFrame.")

    # Aggregate total views by category
    category_views = (
        df.groupby("category_name")["views"].sum().sort_values(ascending=False)
    )

    # Plot
    plt.figure(figsize=(12, 6))
    sns.barplot(x=category_views.index, y=category_views.values, palette="viridis")
    plt.xticks(rotation=45, ha="right")
    plt.title("Top Categories by Total Views")
    plt.xlabel("Category")
    plt.ylabel("Total Views")
    plt.tight_layout()

    # Save the figure
    plt.savefig(output_path)
    print(f"Plot saved to {output_path}")


def plot_top_channels(
    df: pd.DataFrame,
    save_dir: str,
    metric: str = "likes",
    output_path: str = "top10_channels.png",
) -> None:
    """
    Plot top 10 YouTube channels by total likes or views.

    Args:
        df (pd.DataFrame)
        metric (str): 'likes' or 'views' (default 'likes').

    Raises:
        ValueError: If metric is not 'likes' or 'views'.
    """

    output_path = os.path.join(save_dir, output_path)
    if metric not in ["likes", "views"]:
        raise ValueError("Metric must be either 'likes' or 'views'")

    # Aggregate total metric by channel_title
    agg_df = df.groupby("channel_title")[metric].sum().reset_index()

    # Get top 10 channels by metric
    top10 = agg_df.nlargest(10, metric)

    # Plot
    plt.figure(figsize=(12, 7))
    sns.barplot(x=metric, y="channel_title", data=top10, palette="viridis")
    plt.title(f"Top 10 Channels by Total {metric.capitalize()}")
    plt.xlabel(f"Total {metric.capitalize()}")
    plt.ylabel("Channel Title")
    plt.tight_layout()

    # Save the figure
    plt.savefig(output_path)
    print(f"Plot saved to {output_path}")
