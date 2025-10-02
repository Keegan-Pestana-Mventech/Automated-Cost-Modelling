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
            "rate": [],
        }

        self.inspection_log: str = ""

    def set_excel_file(self, filepath: Path):
        """Update the Excel file path"""
        self.excel_file = filepath

    def export_aggregated_data(self) -> Path:
        """Export final aggregated dataframe to Excel, clearing the directory first."""
        # Clear previous output files from the directory
        for file in self.output_directory.iterdir():
            if file.is_file() and file.name.endswith("_aggregated.xlsx"):
                file.unlink()

        if self.final_dataframe is None:
            raise ValueError("Final DataFrame is not set. Cannot export aggregated data.")

        safe_name = self.sheet_name.replace(" ", "_")
        export_path = self.output_directory / f"{safe_name}_aggregated.xlsx"

        # Convert Polars DataFrame -> Pandas -> Excel
        df_pandas = self.final_dataframe.to_pandas()
        df_pandas.to_excel(export_path, index=False, engine="openpyxl")

        return export_path

    def export_raw_data(self, df: pl.DataFrame) -> Path:
        """Exports the raw DataFrame with SI conversions to an Excel file."""
        if df is None:
            raise ValueError("DataFrame to export cannot be None.")
        
        if not self.sheet_name:
            raise ValueError("Sheet name is not set. Cannot create a unique filename.")

        safe_name = self.sheet_name.replace(" ", "_")
        export_path = self.output_directory / f"{safe_name}_dataframe_raw.xlsx"

        # Convert Polars DataFrame -> Pandas -> Excel
        df_pandas = df.to_pandas()
        df_pandas.to_excel(export_path, index=False, engine="openpyxl")

        return export_path

    def export_qa_data(self, df: pl.DataFrame) -> Path:
        """Exports the rate variability QA DataFrame to an Excel file."""
        if df is None:
            raise ValueError("QA DataFrame to export cannot be None.")
        
        if not self.sheet_name:
            raise ValueError("Sheet name is not set. Cannot create a unique filename.")

        safe_name = self.sheet_name.replace(" ", "_")
        export_path = self.output_directory / f"{safe_name}_rate_variability_qa.xlsx"
        
        # Overwrite previous QA file if it exists
        if export_path.exists():
            export_path.unlink()

        df_pandas = df.to_pandas()
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

        return export_path