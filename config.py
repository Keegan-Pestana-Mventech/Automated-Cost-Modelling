from pathlib import Path

# The name of the Excel file to be processed.
# This file should be in the same directory as main.py.
EXCEL_FILE = "REDISTRIBUTED MY26 Deswik Dump Summary Mine Physicals 22 August 2025.xlsx"

# The directory where output files (checkpoints, logs, final data) will be saved.
# This directory will be created if it doesn't exist.
OUTPUT_DIRECTORY = Path("data")
