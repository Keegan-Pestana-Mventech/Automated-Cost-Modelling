import polars as pl
import logging
from typing import List, Optional
import config

logger = logging.getLogger(__name__)


def _validate_columns(
    df: pl.DataFrame, grouping_cols: List[str], start_date_col: str, driver_col: str
) -> None:
    """
    Ensure all required columns exist in the DataFrame.
    
    Args:
        df: Input DataFrame
        grouping_cols: Columns to group by. These are selected by user (location, activity)
        start_date_col:  Name of the date column
        driver_col: Name of the numeric column to aggregate
    """
    required_cols = set(grouping_cols + [start_date_col, driver_col])
    available_cols = set(df.columns)

    # Set difference operation to find columns that are required but missing.
    missing = required_cols - available_cols
    if missing:
        # Sort missing and available columns for deterministic error messages
        # This makes debugging and testing more reliable
        raise ValueError(
            f"Missing required columns in DataFrame: {', '.join(sorted(missing))}. "
            f"Available columns: {', '.join(sorted(available_cols))}"
        )


def _generate_id_column(df: pl.DataFrame, grouping_cols: List[str]) -> pl.DataFrame:
    """
    Generate an ID column by concatenating grouping column values with hyphens.
    
    The ID column provides a unique identifier for each row based on the combination
    of grouping values. This is useful for:
    - Tracking individual records through transformations
    - Creating human-readable composite keys
    - Debugging and data validation
    
    Args:
        df: Input DataFrame with grouping columns
        grouping_cols: List of column names to concatenate
    
    Returns:
        DataFrame with new 'ID' column prepended
        
    Example:
        Input:
            Name   | Age | Gender
            Keegan | 25  | Male
            
        Output (with grouping_cols=['Name', 'Age', 'Gender']):
            ID              | Name   | Age | Gender
            Keegan-25-Male  | Keegan | 25  | Male
    """
    if not grouping_cols:
        # If no grouping columns, create a simple sequential ID
        logger.warning("No grouping columns provided for ID generation. Using row numbers.")
        return df.with_columns(
            pl.arange(0, df.height).cast(pl.Utf8).alias("ID")
        ).select(["ID"] + df.columns)
    
    # Convert all grouping columns to strings and concatenate with hyphen separator
    # We use pl.concat_str which handles null values gracefully (converts to empty string)
    id_expr = pl.concat_str(
        [pl.col(col).cast(pl.Utf8) for col in grouping_cols],
        separator="-"
    ).alias("ID")
    
    # Add ID column and reorder so ID is first
    df_with_id = df.with_columns(id_expr)
    
    # Reorder columns: ID first, then original columns
    return df_with_id.select(["ID"] + df.columns)


def _preprocess_dataframe(
    df: pl.DataFrame, 
    grouping_cols: List[str], 
    start_date_col: str, 
    driver_col: str,
    date_trunc_unit: str
) -> pl.DataFrame:
    """
    Prepare the DataFrame for aggregation:
    - Parse the start date column into proper datetime values (handles both string and datetime types)
    - Cast the driver column to float and replace nulls with 0
    - Truncate parsed dates to a configured period unit (e.g. "1mo" for monthly aggregation)
    - Convert month_period to standardized string format ("%Y-%m") for consistent column naming
    - Drop rows that have invalid or missing dates
    
    Args:
        df: Input DataFrame
        grouping_cols: Columns to group by
        start_date_col: Name of the date column
        driver_col: Name of the numeric column to aggregate
        date_trunc_unit: Time unit for truncation (e.g., "1mo", "1w", "1q")
    
    Returns:
        Preprocessed DataFrame with parsed_date, numeric_driver, and month_period columns
    """

    # Convert the eager DataFrame into a LazyFrame, with our selected columns.
    # A LazyFrame allows you to "describe" a sequence of transformations without executing them immediately.
    # Polars will optimize the query plan and only run it when .collect() is called at the end.
    lazy_df = df.lazy().select(
        grouping_cols + [start_date_col, driver_col]
    )
 
    # Parse the date column into a proper datetime type.
    # We need to handle both string and datetime columns robustly.
    # The approach:
    # - Check the dtype of the column
    # - If it's already Date or Datetime, cast to Datetime
    # - If it's a string, parse it to Datetime
    # - For any other type, the operation will produce null values
    
    # Get the dtype of the start_date_col to determine parsing strategy
    start_col_dtype = df.schema[start_date_col]
    
    if start_col_dtype in (pl.Date, pl.Datetime):
        # Column is already a temporal type, just ensure it's Datetime
        parsed_and_cast = lazy_df.with_columns(
            pl.col(start_date_col).cast(pl.Datetime).alias("parsed_date"),
            pl.col(driver_col).cast(pl.Float64, strict=False).fill_null(0).alias("numeric_driver"),
        )
    else:
        # Column is likely a string, parse it to datetime
        # "strict=False" means invalid parses will become null instead of erroring
        parsed_and_cast = lazy_df.with_columns(
            pl.col(start_date_col).str.to_datetime(strict=False).alias("parsed_date"),
            pl.col(driver_col).cast(pl.Float64, strict=False).fill_null(0).alias("numeric_driver"),
        )

    # Remove any rows where the date could not be parsed (i.e., parsed_date is null).
    filtered = parsed_and_cast.filter(
        pl.col("parsed_date").is_not_null()
    )

    # Truncate the parsed date into a fixed time period, then convert to standardized string format.
    # The date_trunc_unit parameter (e.g., "1mo") determines the aggregation period.
    # All dates will be snapped to the start of the period (e.g., first day of the month).
    # We then convert to "%Y-%m" format for consistent, sortable column names.
   
    truncated = filtered.with_columns(
        pl.col("parsed_date")
            .dt.truncate(date_trunc_unit)
            .dt.strftime("%Y-%m")  # Convert to standardized string format
            .alias("month_period")
    )

    # Execute the lazy query plan and return a regular eager DataFrame.
    # Until this point, no actual computation has occurred.
    return truncated.collect()


def _pivot_monthly(df: pl.DataFrame, grouping_cols: List[str]) -> pl.DataFrame:
    """
    Pivot the numeric driver values into monthly columns.
    
    Args:
        df: Preprocessed DataFrame with month_period column
        grouping_cols: Columns to use as pivot index
    
    Returns:
        Pivoted DataFrame with one column per month period
        
        Result structure: If you start with:
        Region | Product | month_period | numeric_driver
        North  | A       | 2024-01      | 100
        North  | A       | 2024-02      | 150
        
        You get:
        Region | Product | 2024-01 | 2024-02
        North  | A       | 100     | 150
    """
    pivoted = df.pivot(
        values="numeric_driver",
        index=grouping_cols,
        on="month_period",
        aggregate_function="sum",
    )

    return pivoted.sort(grouping_cols)


def aggregate_data(
    df: pl.DataFrame, 
    grouping_cols: List[str], 
    start_date_col: str, 
    driver_col: str,
    date_trunc_unit: Optional[str] = None
) -> pl.DataFrame:
    """
    Perform time-series aggregation with monthly pivoting and ID generation.

    Steps:
    1. Validate required columns exist
    2. Preprocess (parse dates, cast drivers, truncate to configured unit)
    3. Pivot values into monthly columns
    4. Generate ID column from grouping columns

    Args:
        df: Input DataFrame containing the data to aggregate
        grouping_cols: Column names to group by (e.g., location, activity)
        start_date_col: Column containing the start date values (string or datetime type)
        driver_col: Column containing numeric driver values to sum
        date_trunc_unit: Time unit for date truncation (e.g., "1mo", "1w", "1q").
                        If None, defaults to config.DATE_TRUNC_UNIT

    Returns:
        pl.DataFrame: Pivoted DataFrame with ID column first, followed by grouping columns,
                     then monthly columns (format: "YYYY-MM").
                     
                     The ID column contains hyphen-separated concatenation of grouping values.
                     
                     Example output structure:
                     ID              | Name   | Age | Gender | 2024-01 | 2024-02
                     Keegan-25-Male  | Keegan | 25  | Male   | 100     | 150
                     
                     Note: If no valid data remains after preprocessing, returns an empty DataFrame
                     with ID and grouping columns.

    Raises:
        ValueError: If required columns are missing from input DataFrame
        Exception: For unexpected errors during processing
    """
    try:
        # Use provided date_trunc_unit or fall back to config default
        # This allows for flexible aggregation periods while maintaining backward compatibility
        trunc_unit = date_trunc_unit or config.DATE_TRUNC_UNIT
        
        logger.info(
            f"Starting aggregation. Grouping by: {grouping_cols}, "
            f"Truncating to: {trunc_unit}"
        )

        # 1. Validate input
        _validate_columns(df, grouping_cols, start_date_col, driver_col)

        # 2. Preprocess data
        df_transformed = _preprocess_dataframe(
            df, grouping_cols, start_date_col, driver_col, trunc_unit
        )
        if df_transformed.height == 0:
            logger.warning(
                "No valid data after preprocessing. Returning empty DataFrame. "
                "This typically means all dates were invalid or missing."
            )
            # Return empty frame with ID and grouping columns
            empty_df = pl.DataFrame({col: [] for col in grouping_cols})
            return _generate_id_column(empty_df, grouping_cols)

        # 3. Pivot to monthly columns
        pivoted = _pivot_monthly(df_transformed, grouping_cols)

        # Identify month columns (all columns except grouping columns)
        # Month columns are in "%Y-%m" format and will sort lexicographically in chronological order
        month_cols = sorted(
            [col for col in pivoted.columns if col not in grouping_cols]
        )
        if not month_cols:
            logger.warning("No month columns generated after pivot.")
            result = pivoted.select(grouping_cols)
            return _generate_id_column(result, grouping_cols)

        # Ensure all month columns are numeric before we attempt to sum them
        # This is a defensive check to prevent cryptic errors from sum_horizontal
        for col in month_cols:
            if pivoted.schema[col] not in (pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.UInt64, pl.UInt32):
                raise TypeError(
                    f"Expected month column '{col}' to be numeric, but got {pivoted.schema[col]}. "
                    f"This indicates an issue with the pivot operation."
                )

        pivoted = pivoted.select(grouping_cols + month_cols).fill_null(0)

        # 4. Generate ID column from grouping columns
        # This creates a composite key by concatenating all grouping values with hyphens
        # The ID column will be positioned as the first column in the output
        pivoted_with_id = _generate_id_column(pivoted, grouping_cols)

        logger.info(
            f"Aggregation complete. Result shape: {pivoted_with_id.shape}. "
            f"ID column generated from {len(grouping_cols)} grouping columns."
        )
        return pivoted_with_id

    except Exception as e:
        logger.error(f"An error occurred during data aggregation: {e}")
        raise