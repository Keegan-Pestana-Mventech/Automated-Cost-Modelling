from pathlib import Path
from typing import Dict, List, Optional
import polars as pl


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
        """Export final aggregated dataframe to CSV"""
        for file in self.output_directory.iterdir():
            if file.is_file():
                file.unlink()

        safe_name = self.sheet_name.replace(" ", "_")
        export_path = self.output_directory / f"{safe_name}_aggregated.csv"
        self.final_dataframe.write_csv(export_path)
        return export_path
