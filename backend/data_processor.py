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

    # Convert the eager DataFrame into a LazyFrame, with our selected coloumns.
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


def _calculate_totals(
    df: pl.DataFrame, grouping_cols: List[str], month_cols: List[str]
) -> pl.DataFrame:
    """
    Add row totals and a grand total row (if grouping columns are present).
    
    Args:
        df: Pivoted DataFrame
        grouping_cols: Grouping column names
        month_cols: List of month column names (already validated as numeric)
    
    Returns:
        DataFrame with Total column and GRAND TOTAL row appended
    """
    # Add row total by summing across all month columns horizontally
    # sum_horizontal requires all columns to be numeric, which is ensured by caller
    df = df.with_columns(Total=pl.sum_horizontal(month_cols))

    if not grouping_cols:
        return df

    # Calculate grand totals across all numeric columns
    # This produces a single-row DataFrame with the sum of each column
    numeric_totals = df.select(month_cols + ["Total"]).sum()

    # Build the summary row
    # First grouping column gets "GRAND TOTAL" label, others get empty strings
    grand_total_data = {grouping_cols[0]: "GRAND TOTAL"}
    for col in grouping_cols[1:]:
        grand_total_data[col] = ""

    # Add the computed totals for each month and the Total column
    for col in month_cols + ["Total"]:
        grand_total_data[col] = numeric_totals[col][0]

    total_row = pl.DataFrame([grand_total_data])
    return pl.concat([df, total_row], how="vertical")


def aggregate_data(
    df: pl.DataFrame, 
    grouping_cols: List[str], 
    start_date_col: str, 
    driver_col: str,
    date_trunc_unit: Optional[str] = None
) -> pl.DataFrame:
    """
    Perform time-series aggregation with monthly pivoting.

    Steps:
    1. Validate required columns exist
    2. Preprocess (parse dates, cast drivers, truncate to configured unit)
    3. Pivot values into monthly columns

    Args:
        df: Input DataFrame containing the data to aggregate
        grouping_cols: Column names to group by (e.g., location, activity)
        start_date_col: Column containing the start date values (string or datetime type)
        driver_col: Column containing numeric driver values to sum
        date_trunc_unit: Time unit for date truncation (e.g., "1mo", "1w", "1q").
                        If None, defaults to config.DATE_TRUNC_UNIT

    Returns:
        pl.DataFrame: Pivoted DataFrame with monthly columns (format: "YYYY-MM").
                     
                     Note: If no valid data remains after preprocessing, returns an empty DataFrame
                     with only grouping columns.

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
            # Return empty frame with just the grouping columns
            return pl.DataFrame({col: [] for col in grouping_cols})

        # 3. Pivot to monthly columns
        pivoted = _pivot_monthly(df_transformed, grouping_cols)

        # Identify month columns (all columns except grouping columns)
        # Month columns are in "%Y-%m" format and will sort lexicographically in chronological order
        month_cols = sorted(
            [col for col in pivoted.columns if col not in grouping_cols]
        )
        if not month_cols:
            logger.warning("No month columns generated after pivot.")
            return pivoted.select(grouping_cols)

        # Ensure all month columns are numeric before we attempt to sum them
        # This is a defensive check to prevent cryptic errors from sum_horizontal
        for col in month_cols:
            if pivoted.schema[col] not in (pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.UInt64, pl.UInt32):
                raise TypeError(
                    f"Expected month column '{col}' to be numeric, but got {pivoted.schema[col]}. "
                    f"This indicates an issue with the pivot operation."
                )

        pivoted = pivoted.select(grouping_cols + month_cols).fill_null(0)

        # 4. Totals are no longer added to the DataFrame per user request.

        logger.info(f"Aggregation complete. Result shape: {pivoted.shape}")
        return pivoted

    except Exception as e:
        logger.error(f"An error occurred during data aggregation: {e}")
        raise