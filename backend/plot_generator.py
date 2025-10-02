import matplotlib.pyplot as plt
import pandas as pd
import polars as pl
import logging
from typing import List, Dict, Any, Tuple

import config

logger = logging.getLogger(__name__)


def _extract_series_data(
    df: pl.DataFrame, entry_index: int, grouping_cols: List[str]
) -> Tuple[List[str], List[float]]:
    """
    Helper function to extract plottable month and value data for a single series.
    """
    # Exclude non-data columns
    exclude_cols = grouping_cols + ["ID", "Total"]
    month_cols = [col for col in df.columns if col not in exclude_cols]

    values: List[float] = []
    valid_months: List[str] = []

    # Ensure the row exists
    if 0 <= entry_index < len(df):
        row = df.row(entry_index, named=True)
        for month_col in month_cols:
            value = row.get(month_col)
            if value is not None:
                values.append(float(value))
                valid_months.append(month_col)

    return valid_months, values


def generate_comparison_plot(
    original_df: pl.DataFrame,
    smoothed_df: pl.DataFrame,
    entry_index: int,
    selected_entry_label: str,
    grouping_cols: List[str],
    driver_col_name: str,
    plot_settings: Dict[str, Any],
    output_filename: str,
) -> plt.Figure:
    """
    Generates a single plot comparing the original and smoothed data series.

    Args:
        original_df: DataFrame with the original aggregated data.
        smoothed_df: DataFrame with the stockpile-smoothed data.
        entry_index: The row index to plot from both DataFrames.
        selected_entry_label: The label for the plot title.
        grouping_cols: Column names used for grouping.
        driver_col_name: The name of the driver column for the y-axis label.
        plot_settings: Dictionary with plot styling options.
        output_filename: The filename to save the plot to.

    Returns:
        plt.Figure: The generated Matplotlib figure.
    """
    figure = plt.Figure(figsize=config.PLOT_FIGSIZE, dpi=config.PLOT_DPI)
    ax = figure.add_subplot(111)

    # Extract data for both original and smoothed series
    original_months, original_values = _extract_series_data(
        original_df, entry_index, grouping_cols
    )
    smoothed_months, smoothed_values = _extract_series_data(
        smoothed_df, entry_index, grouping_cols
    )

    # Create a unified timeline to correctly align both datasets
    all_months = sorted(list(set(original_months + smoothed_months)))
    if not all_months:
        logger.warning("No data available to plot for comparison.")
        ax.text(0.5, 0.5, "No data to plot", ha="center", va="center")
        return figure

    # Map months to their index on the unified timeline
    month_to_x = {month: i for i, month in enumerate(all_months)}
    x_positions = range(len(all_months))

    # Create full-length data arrays, defaulting to zero
    aligned_original_values = [0.0] * len(all_months)
    for month, value in zip(original_months, original_values):
        aligned_original_values[month_to_x[month]] = value

    aligned_smoothed_values = [0.0] * len(all_months)
    for month, value in zip(smoothed_months, smoothed_values):
        aligned_smoothed_values[month_to_x[month]] = value

    # Plotting
    # 1. Original data as bars
    ax.plot(
        x_positions,
        aligned_original_values,
        color=plot_settings.get("color", "#2E86AB"),
        alpha=0.6,
        marker=plot_settings.get("marker", "o"),
        linewidth=plot_settings.get("linewidth", 2.5),
        markersize=plot_settings.get("markersize", 6),
        label="Original",
    )

    # 2. Smoothed data as a line
    ax.plot(
        x_positions,
        aligned_smoothed_values,
        color=plot_settings.get("comparison_color", "#D9534F"),
        marker=plot_settings.get("marker", "o"),
        linewidth=plot_settings.get("linewidth", 2.5),
        markersize=plot_settings.get("markersize", 6),
        label="Smoothed",
    )

    # Formatting
    formatted_months = [pd.to_datetime(m).strftime("%b %Y") for m in all_months]
    ax.set_xticks(x_positions)
    ax.set_xticklabels(formatted_months, rotation=45, ha="right")
    ax.set_ylabel(driver_col_name)
    ax.legend()

    if plot_settings.get("grid", True):
        ax.grid(True, linestyle="--", alpha=0.4)

    title = f"Original vs. Smoothed Profile for: {selected_entry_label}"
    truncated_title = (
        title[: config.MAX_TITLE_LENGTH] + "..."
        if len(title) > config.MAX_TITLE_LENGTH
        else title
    )
    ax.set_title(truncated_title)

    figure.tight_layout()

    # Save the figure
    plot_path = config.OUTPUT_DIRECTORY / config.PLOT_OUTPUT_SUBDIR / output_filename
    figure.savefig(plot_path)
    logger.info(f"Comparison plot saved to: {plot_path}")
    plt.close(figure) # Release memory

    return figure


def generate_plot(
    df: pl.DataFrame,
    entry_index: int,
    selected_entry_label: str,
    grouping_cols: List[str],
    driver_col_name: str,
    plot_settings: Dict[str, Any],
) -> plt.Figure:
    """
    Generate a plot for a selected entry's monthly driver profile.
    (This function is kept for plotting single dataframes).
    """
    # Guard against invalid entry_index
    if not 0 <= entry_index < len(df):
        figure = plt.Figure(figsize=config.PLOT_FIGSIZE, dpi=config.PLOT_DPI)
        ax = figure.add_subplot(111)
        ax.text(0.5, 0.5, "Invalid entry index", ha="center", va="center")
        ax.set_title("Error")
        figure.tight_layout()
        return figure

    figure = plt.Figure(figsize=config.PLOT_FIGSIZE, dpi=config.PLOT_DPI)
    ax = figure.add_subplot(111)

    valid_months, values = _extract_series_data(df, entry_index, grouping_cols)

    if values:
        x_positions = range(len(valid_months))
        plot_type = plot_settings.get("plot_type", "line")
        color = plot_settings.get("color", "#2E86AB")
        
        if plot_type == "line":
            ax.plot(x_positions, values, color=color, **plot_settings)
        elif plot_type == "bar":
            ax.bar(x_positions, values, color=color, alpha=0.7)
        # ... other plot types ...

        for x, y in zip(x_positions, values):
            if y != 0:
                ax.annotate(f"{y:,.0f}", (x, y), textcoords="offset points", xytext=(0, 10), ha="center")

        formatted_months = [pd.to_datetime(m).strftime("%b %Y") for m in valid_months]
        ax.set_xticks(x_positions)
        ax.set_xticklabels(formatted_months, rotation=45, ha="right")
        ax.set_ylabel(driver_col_name)

        if plot_settings.get("grid", True):
            ax.grid(True, linestyle="--", alpha=0.4)

        title = f"Monthly Driver Profile for: {selected_entry_label}"
        truncated_title = (
            title[: config.MAX_TITLE_LENGTH] + "..."
            if len(title) > config.MAX_TITLE_LENGTH
            else title
        )
        ax.set_title(truncated_title)

    figure.tight_layout()
    return figure