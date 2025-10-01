from pathlib import Path
from typing import Dict, List, Optional
import polars as pl
import pandas as pd


class AppState:
    """Centralized application state management"""

    def __init__(self, excel_file: Path, output_directory: Path):
        self.excel_file = excel_file
        self.output_directory = output_directory
        self.output_directory.mkdir(exist_ok=True)

        self.sheet_name: Optional[str] = None
        self.df: Optional[pl.DataFrame] = None
        self.final_dataframe: Optional[pl.DataFrame] = None
        self.driver_col: Optional[str] = None

        self.selected_columns: Dict[str, List[str]] = {
            "location": [],
            "activity": [],
            "timing": [],
            "drivers": [],
        }

        self.inspection_log: str = ""

    def set_excel_file(self, filepath: Path):
        """Update the Excel file path"""
        self.excel_file = filepath

    def export_aggregated_data(self) -> Path:
        """Export final aggregated dataframe to Excel, clearing the directory first."""
        # Clear previous output files from the directory
        for file in self.output_directory.iterdir():
            if file.is_file():
                file.unlink()

        if self.final_dataframe is None:
            raise ValueError("Final DataFrame is not set. Cannot export aggregated data.")

        safe_name = self.sheet_name.replace(" ", "_")
        export_path = self.output_directory / f"{safe_name}_aggregated.xlsx"

        # Convert Polars DataFrame -> Pandas -> Excel
        df_pandas = self.final_dataframe.to_pandas()
        df_pandas.to_excel(export_path, index=False, engine="openpyxl")

        return export_path

    def export_transformed_data(self, df: pl.DataFrame) -> Path:
        """Exports the transformed DataFrame to an Excel file."""
        if df is None:
            raise ValueError("DataFrame to export cannot be None.")
        
        if not self.sheet_name:
            raise ValueError("Sheet name is not set. Cannot create a unique filename.")

        safe_name = self.sheet_name.replace(" ", "_")
        export_path = self.output_directory / f"{safe_name}_transformed.xlsx"

        # Convert Polars DataFrame -> Pandas -> Excel
        df_pandas = df.to_pandas()
        df_pandas.to_excel(export_path, index=False, engine="openpyxl")

        # Note: This function intentionally does not clear the output directory
        # to allow both transformed and aggregated files to coexist.
        # The directory is cleared by the final `export_aggregated_data` call.
        return export_path
