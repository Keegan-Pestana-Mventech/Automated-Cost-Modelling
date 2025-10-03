from datetime import datetime
import polars as pl
import config


class DataFrameInspector:
    """Lightweight inspection utilities for DataFrame operations."""

    @staticmethod
    def log_step(df: pl.DataFrame, step: str, details: str = "") -> str:
        """
        Generate a formatted log message for DataFrame processing steps.

        Args:
            df: DataFrame being processed
            step: Description of the processing step
            details: Additional details to include in the log message

        Returns:
            str: Formatted log message with timestamp and DataFrame shape
        """
        timestamp = datetime.now().strftime("%H:%M:%S")
        return (
            f"[{timestamp}] {step}: {df.shape[0]} rows, {df.shape[1]} cols {details}\n"
        )

    @staticmethod
    def inspect_dataframe(df: pl.DataFrame, step_name: str) -> str:
        """
        Generate a comprehensive inspection report for a DataFrame.

        Args:
            df: DataFrame to inspect
            step_name: Name of the processing step for context

        Returns:
            str: Formatted inspection report with shape, memory usage, and column details
        """
        lines = [
            f"\n{'=' * config.REPORT_WIDTH}",
            f"{step_name.upper()}",
            f"{'=' * config.REPORT_WIDTH}",
            f"Shape: {df.shape[0]} rows × {df.shape[1]} columns",
            f"Memory: {df.estimated_size('mb'):.2f} MB\n",
            "COLUMNS:",
        ]

        for i, col in enumerate(df.columns, 1):
            lines.append(
                f"  {i:2d}. {col:<30} ({str(df[col].dtype):<10}) - {df[col].null_count()} nulls"
            )

        lines.extend(
            [
                f"\nPREVIEW:\n{df.head(3)}\n",
                f"{'=' * config.REPORT_WIDTH}\n",
            ]
        )

        report = "\n".join(lines)
        print(report)
        return report
