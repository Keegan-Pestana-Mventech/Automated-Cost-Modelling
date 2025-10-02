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
    "rate": ["Rate"],  # Added Rate category
}

# =============================================================================
# RATE HANDLING & VALIDATION
# =============================================================================
# The core assumption of the aggregation is that rates are constant for a given
# group (e.g., for a specific Mine, Zone, and Activity). This section
# configures how the application validates this assumption.

# If True, the aggregation process will be stopped with an error if rate
# variability is detected within any group. This enforces strict data quality.
# If False, a warning will be shown to the user, who can then choose to
# proceed. If they proceed, the first rate encountered for each group will be
# used as the representative "standard rate".
BLOCK_ON_VARIABILITY = False

# Float tolerance for comparing SI rate values to check for consistency.
# Rates are considered different if abs(rate1 - rate2) > RATE_EPSILON.
# This prevents false positives from minor floating-point inaccuracies.
RATE_EPSILON = 0.01

# Format string for the new SI Rate column name created during unit conversion.
# The '{}' will be replaced with the original rate column name.
RATE_COLUMN_ALIAS = "SI {}"

# If True, a detailed Excel file is automatically generated and saved when
# rate variability is detected. This report lists all groups that failed the
# consistency check, helping users diagnose data quality issues.
QA_EXPORT_ENABLED = True


# =============================================================================
# DATA PROCESSING
# =============================================================================
# The time unit for truncating dates for monthly aggregation.
# See polars documentation for options (e.g., "1mo", "1w", "1d").
DATE_TRUNC_UNIT = "1mo"

# Default thresholds for stockpile smoothing, keyed by driver column name.
# These are initial values and can be adjusted in the UI.
DRIVER_THRESHOLDS = {
    "Linear Meters": 100.0
    # Add other potential driver columns and their default thresholds here
}


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
    "Grape": "#6A4C93",
    "Sunny Yellow": "#FFCA3A",
    "Ruby Red": "#D62828",
    "Emerald Green": "#04A777",
}

# Style for filtered data series when overlaying with unfiltered data.
FILTERED_PLOT_STYLE = {
    "linestyle": "--",
    "linewidth": 2,
}

# Default settings for the plot view (kept for potential future use).
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
    ("Rate", "rate", "Rate values associated with activities (e.g., $/ton)"),
]

# Number of columns for the checkbox grid in the column selection UI.
COLUMN_SELECTION_GRID_COLUMNS = 5