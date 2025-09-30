import polars as pl
import logging
import os

logger = logging.getLogger(__name__)


def load_excel(filepath: str, sheet_name: str) -> pl.DataFrame:
    """
    Load an Excel sheet with schema inference disabled to handle data type issues.

    Args:
        filepath: Path to the Excel file to load
        sheet_name: Name of the sheet within the Excel file to load

    Returns:
        pl.DataFrame: DataFrame containing the loaded Excel data with all columns as strings

    Raises:
        FileNotFoundError: If the specified file does not exist
        ValueError: If file extension is invalid or sheet name exceeds length limits
    """
    # 1. Validate the file exists.
    if not os.path.exists(filepath):
        error_msg = f"File not found at path: {filepath}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    # 2. Validate extension is .xlsx or .xls.
    allowed_extensions = (".xlsx", ".xls")
    file_extension = os.path.splitext(filepath)[1].lower()
    if file_extension not in allowed_extensions:
        error_msg = f"Invalid file extension for file: {filepath}. Must be one of {allowed_extensions}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    # 3. Validate sheet name length ≤ 100 characters.
    max_sheet_name_length = 100
    if len(sheet_name) > max_sheet_name_length:
        error_msg = (
            f"Sheet name exceeds maximum length of {max_sheet_name_length} characters."
        )
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info(f"All validations passed for file: {filepath}")

    # After validation, proceed with pl.read_excel.
    # Load with schema inference disabled to force all columns as strings
    try:
        df = pl.read_excel(filepath, sheet_name=sheet_name, infer_schema_length=0)
        logger.info(
            "Sheet loaded with schema inference disabled (all columns as strings)"
        )
        return df
    except Exception as e:
        # Catch potential errors from polars itself (e.g., sheet not found)
        logger.error(
            f"Failed to read Excel file '{filepath}' sheet '{sheet_name}': {e}"
        )
        raise
