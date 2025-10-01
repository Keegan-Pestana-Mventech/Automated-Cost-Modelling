import matplotlib.pyplot as plt
import pandas as pd
import polars as pl
import logging
from typing import List, Dict, Any
import config

logger = logging.getLogger(__name__)


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

    Args:
        df: DataFrame containing the aggregated data
        entry_index: Index of the selected entry in the DataFrame
        selected_entry_label: Label for the selected entry to display in title
        grouping_cols: Column names used for grouping in the aggregation
        driver_col_name: The name of the driver column for the y-axis label.
        plot_settings: Dictionary containing plot customization options

    Returns:
        plt.Figure: Matplotlib figure object containing the generated plot
    """
    # Guard against invalid entry_index
    if not 0 <= entry_index < len(df):
        figure = plt.Figure(figsize=config.PLOT_FIGSIZE, dpi=config.PLOT_DPI)
        ax = figure.add_subplot(111)
        ax.text(
            0.5,
            0.5,
            "Invalid entry index",
            ha="center",
            va="center",
            fontsize=12,
            transform=ax.transAxes,
        )
        ax.set_title("Error")
        figure.tight_layout()
        return figure

    # Guard against plotting the "GRAND TOTAL" row
    if grouping_cols and df[grouping_cols[0]][entry_index] == "GRAND TOTAL":
        figure = plt.Figure(figsize=config.PLOT_FIGSIZE, dpi=config.PLOT_DPI)
        ax = figure.add_subplot(111)
        ax.text(
            0.5,
            0.5,
            "Cannot plot GRAND TOTAL row",
            ha="center",
            va="center",
            fontsize=12,
            transform=ax.transAxes,
        )
        ax.set_title("Plotting Not Applicable")
        figure.tight_layout()
        return figure

    figure = plt.Figure(figsize=config.PLOT_FIGSIZE, dpi=config.PLOT_DPI)
    ax = figure.add_subplot(111)

    # Get monthly columns (exclude grouping columns, 'ID', and 'Total')
    # The 'ID' column is a string identifier and not a plottable value.
    exclude_cols = grouping_cols + ["ID", "Total"]
    month_cols = [
        col for col in df.columns if col not in exclude_cols
    ]

    values: List[float] = []
    valid_months: List[str] = []

    for month_col in month_cols:
        value = df[month_col][entry_index]
        if value is not None:
            values.append(float(value))
            # Format month name for display on the x-axis
            try:
                month_name = pd.to_datetime(month_col).strftime("%b %Y")
                valid_months.append(month_name)
            except (ValueError, TypeError):
                valid_months.append(month_col)

    if values:
        x_positions = range(len(valid_months))

        # Apply plot settings
        plot_type = plot_settings.get("plot_type", "line")
        color = plot_settings.get("color", "#2E86AB")
        marker = plot_settings.get("marker", "o")
        linewidth = plot_settings.get("linewidth", 2)
        markersize = plot_settings.get("markersize", 6)
        grid_visible = plot_settings.get("grid", True)

        # Generate plot based on type
        if plot_type == "line":
            ax.plot(
                x_positions,
                values,
                marker=marker,
                linewidth=linewidth,
                markersize=markersize,
                color=color,
            )
        elif plot_type == "bar":
            ax.bar(x_positions, values, color=color, alpha=0.7)
        elif plot_type == "scatter":
            ax.scatter(
                x_positions,
                values,
                color=color,
                s=markersize * 20,  # Scale for scatter
                alpha=0.7,
            )
        elif plot_type == "step":
            ax.step(x_positions, values, color=color, linewidth=linewidth, where="mid")

        # Add value labels above the points
        for x, y in zip(x_positions, values):
            if y != 0:  # Only label non-zero values for clarity
                ax.annotate(
                    f"{y:,.0f}",
                    (x, y),
                    textcoords="offset points",
                    xytext=(0, 10),
                    ha="center",
                    fontsize=9,
                )

        ax.set_xticks(x_positions)
        ax.set_xticklabels(valid_months, rotation=45, ha="right")
        ax.set_ylabel(driver_col_name)

        if grid_visible:
            ax.grid(True, linestyle="--", alpha=0.4)

        # Truncate title if too long
        title = f"Monthly Driver Profile for: {selected_entry_label}"
        truncated_title = (
            title[: config.MAX_TITLE_LENGTH] + "..."
            if len(title) > config.MAX_TITLE_LENGTH
            else title
        )
        ax.set_title(truncated_title)

        # Display total sum in a text box
        total_sum = sum(values)
        ax.text(
            0.02,
            0.98,
            f"Total: {total_sum:,.0f}",
            transform=ax.transAxes,
            fontsize=12,
            verticalalignment="top",
            bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.8),
        )
    else:
        ax.text(
            0.5,
            0.5,
            "No data available for the selected entry",
            ha="center",
            va="center",
            fontsize=12,
            transform=ax.transAxes,
        )
        ax.set_title(f"No Data for: {selected_entry_label}")

    figure.tight_layout()
    return figure