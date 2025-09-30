from pathlib import Path

# The name of the Excel file to be processed.
# This file should be in the same directory as main.py.
EXCEL_FILE = "REDISTRIBUTED MY26 Deswik Dump Summary Mine Physicals 22 August 2025.xlsx"

# The directory where output files (checkpoints, logs, final data) will be saved.
# This directory will be created if it doesn't exist.
OUTPUT_DIRECTORY = Path("data")

# Default sheet name to load
DEFAULT_SHEET_NAME = "Deswik Dump"

# Default column selections - saves time when processing files with the same structure
DEFAULT_COLUMN_SELECTIONS = {
    "location": ["Description", "Zone", "Mine"],
    "activity": ["Excavation Type", "Material Type"],
    "timing": ["Start", "Finish"],
    "drivers": ["Linear Meters"],
}

# =============================================================================
# DATA PROCESSING
# =============================================================================
# The time unit for truncating dates for monthly aggregation.
# See polars documentation for options (e.g., "1mo", "1w", "1d").
DATE_TRUNC_UNIT = "1mo"


# =============================================================================
# PLOTTING
# =============================================================================
# Default figure size (width, height) in inches for plots.
PLOT_FIGSIZE = (10, 6)

# Dots per inch for plot figures.
PLOT_DPI = 100

# Maximum length for plot titles before truncation.
MAX_TITLE_LENGTH = 70

# Directory within OUTPUT_DIRECTORY to save exported plots.
PLOT_OUTPUT_SUBDIR = "plots"

# Predefined color palette for plots.
PLOT_COLOR_MAP = {
    "Ocean Blue": "#2E86AB",
    "Fuchsia": "#A23B72",
    "Tangerine": "#F18F01",
    "Crimson": "#C73E1D",
    "Sky Blue": "#3F7CAC",
    "Forest Green": "#2BA84A",
}

# Default settings for the plot view.
DEFAULT_PLOT_SETTINGS = {
    "plot_type": "line",
    "color": PLOT_COLOR_MAP["Ocean Blue"],
    "marker": "o",
    "linewidth": 2,
    "markersize": 6,
    "grid": True,
}


# =============================================================================
# UI & STYLING
# =============================================================================
# Initial window size.
WINDOW_GEOMETRY = "1200x900"
WINDOW_MIN_SIZE = (1100, 800)

# Centralized font configuration.
FONT_CONFIG = {
    "header": ("Arial", 16, "bold"),
    "stage": ("Arial", 12),
    "file_path": ("Arial", 10),
    "label": ("Arial", 11),
    "entry": ("Arial", 11),
    "description": ("Arial", 10),
    "log": ("Consolas", 9),
    "small_info": ("Arial", 9),
    "plot_header": ("Arial", 14, "bold"),
}

# Centralized color configuration.
COLOR_CONFIG = {
    "bg_main": "#f0f0f0",
    "stage_fg": "#666666",
    "file_path_fg": "#888888",
    "link_fg": "#0066cc",
}

# Width for separators in the inspection log.
REPORT_WIDTH = 70

# Defines the tabs for column selection.
# Structure: (Tab Title, internal_key, description_text)
COLUMN_CATEGORIES = [
    ("Location", "location", "Spatial identifiers (e.g., pit, strip, block)"),
    ("Activity", "activity", "Work type or task (e.g., process, material)"),
    ("Timing", "timing", "Date or scheduling columns (e.g., start/end date)"),
    ("Drivers", "drivers", "Numeric quantities or metrics (e.g., tons, BCMs)"),
]

# Number of columns for the checkbox grid in the column selection UI.
COLUMN_SELECTION_GRID_COLUMNS = 5
