import polars as pl
import logging
from typing import List, Optional, Tuple, Dict, Any
import re
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
    date_trunc_unit: str,
    passthrough_cols: Optional[List[str]] = None
) -> pl.DataFrame:
    """
    Prepare the DataFrame for aggregation, optionally keeping extra columns.
    
    - Parse the start date column into proper datetime values.
    - Cast the driver column to float and replace nulls with 0.
    - Truncate parsed dates to a configured period unit (e.g., "1mo").
    - Convert the period to a standardized string format ("%Y-%m").
    - Drop rows that have invalid or missing dates.
    - Keep specified `passthrough_cols` for later use (e.g., rate validation).
    
    Args:
        df: Input DataFrame.
        grouping_cols: Columns to group by.
        start_date_col: Name of the date column.
        driver_col: Name of the numeric column to aggregate.
        date_trunc_unit: Time unit for truncation (e.g., "1mo", "1w", "1q").
        passthrough_cols: Optional list of other columns to keep in the output.
    
    Returns:
        Preprocessed DataFrame with parsed_date, numeric_driver, month_period,
        and any specified passthrough columns.
    """
    passthrough_cols = passthrough_cols or []
    # Ensure all columns to be used are selected, preventing duplicates
    all_cols_to_select = list(set(grouping_cols + [start_date_col, driver_col] + passthrough_cols))

    # Use a LazyFrame to build the query plan for efficiency.
    lazy_df = df.lazy().select(all_cols_to_select)
 
    # Robustly parse the date column, handling both string and datetime types.
    start_col_dtype = df.schema[start_date_col]
    
    if start_col_dtype in (pl.Date, pl.Datetime):
        parsed_and_cast = lazy_df.with_columns(
            pl.col(start_date_col).cast(pl.Datetime).alias("parsed_date"),
            pl.col(driver_col).cast(pl.Float64, strict=False).fill_null(0).alias("numeric_driver"),
        )
    else:
        parsed_and_cast = lazy_df.with_columns(
            pl.col(start_date_col).str.to_datetime(strict=False).alias("parsed_date"),
            pl.col(driver_col).cast(pl.Float64, strict=False).fill_null(0).alias("numeric_driver"),
        )

    # Remove any rows where the date could not be parsed.
    filtered = parsed_and_cast.filter(
        pl.col("parsed_date").is_not_null()
    )

    # Truncate the date to the specified period and format it as a string.
    truncated = filtered.with_columns(
        pl.col("parsed_date")
            .dt.truncate(date_trunc_unit)
            .dt.strftime("%Y-%m")
            .alias("month_period")
    )

    # Execute the lazy query plan and return the result.
    return truncated.collect()


def _pivot_monthly(df: pl.DataFrame, grouping_cols: List[str]) -> pl.DataFrame:
    """
    Pivot the numeric driver values into monthly columns.
    
    Args:
        df: Preprocessed DataFrame with month_period column.
        grouping_cols: Columns to use as the pivot index.
    
    Returns:
        Pivoted DataFrame with one column per month period.
    """
    pivoted = df.pivot(
        values="numeric_driver",
        index=grouping_cols,
        columns="month_period",
        aggregate_function="sum",
    )
    return pivoted.sort(grouping_cols)


def _is_month_column(col_name: str) -> bool:
    """
    Determine if a column name represents a month period (YYYY-MM format).
    
    This helper prevents non-temporal columns (like rate descriptors) from being
    incorrectly identified as data columns during filtering and aggregation operations.
    
    Args:
        col_name: Column name to check
    
    Returns:
        True if the column matches YYYY-MM format, False otherwise
    """
    month_pattern = re.compile(r'^\d{4}-\d{2}$')
    return bool(month_pattern.match(col_name))


def _validate_rate_consistency(
    df: pl.DataFrame, grouping_cols: List[str], si_rate_col: str
) -> Dict[str, Any]:
    """
    Validates if a rate is constant for each group and extracts a representative rate.

    This function is central to ensuring data quality. It verifies the assumption
    that for any given group (e.g., a specific mine and activity), the rate
    does not change over time.

    Process:
    1.  Extracts a numeric value from the `si_rate_col` string for comparison.
    2.  Counts and reports any rows where the rate could not be parsed.
    3.  For each group, it calculates the number of unique rates and the numeric
        difference between the minimum and maximum rates.
    4.  A group is flagged as "variable" if it has more than one unique rate string
        AND the numeric difference is greater than the `RATE_EPSILON` config value.
    5.  Crucially, it also generates a `rate_descriptors_df` which contains a single
        "standard rate" for EVERY group. This is determined by taking the FIRST
        rate encountered for that group. This ensures that even if variability is
        found and the user chooses to proceed, there is a deterministic rate to display.

    Args:
        df: The preprocessed DataFrame (must be in long format).
        grouping_cols: The columns that define a unique group.
        si_rate_col: The name of the SI rate column to validate.

    Returns:
        A dictionary containing the validation results:
        - "is_consistent" (bool): True if all groups have constant rates.
        - "variable_groups_df" (pl.DataFrame | None): A QA report DataFrame detailing
          the groups with inconsistent rates, or None if all are consistent.
        - "rate_descriptors_df" (pl.DataFrame): A DataFrame with `grouping_cols` and a
          single representative rate (`first_si_rate`) for every group.
        - "message" (str): A human-readable summary of the validation outcome.
        - "unparsable_rate_count" (int): The number of rows where the rate could not be
          parsed into a number, which are excluded from the check.
    """
    logger.info(f"Starting rate consistency validation for column: '{si_rate_col}'")

    # Extract the first valid number from the rate string using regex.
    df_with_numeric_rate = df.with_columns(
        pl.col(si_rate_col)
        .str.extract(r"^(-?\d+\.?\d*)", 1)
        .cast(pl.Float64, strict=False)
        .alias("numeric_si_rate")
    )
    
    # Count rows where rate parsing failed to inform the user of potential data issues.
    unparsable_count = df_with_numeric_rate.filter(pl.col("numeric_si_rate").is_null()).height
    if unparsable_count > 0:
        logger.warning(f"{unparsable_count} rows had unparsable or null rate values and were excluded from consistency validation.")

    df_parsable = df_with_numeric_rate.filter(pl.col("numeric_si_rate").is_not_null())

    if df_parsable.height == 0:
        logger.warning("No numeric rate values found to perform consistency check.")
        return {
            "is_consistent": True, "variable_groups_df": None, "rate_descriptors_df": None,
            "message": "No numeric rates to check.", "unparsable_rate_count": unparsable_count
        }

    # Group by the identifying columns and check for rate variations.
    validation_summary = (
        df_parsable.group_by(grouping_cols)
        .agg(
            pl.n_unique(si_rate_col).alias("n_unique_rates"),
            pl.min("numeric_si_rate").alias("min_rate"),
            pl.max("numeric_si_rate").alias("max_rate"),
            # NEW: Collect all the actual rate strings for the min and max values
            pl.col(si_rate_col).first().alias("example_rate"),
            # NEW: Get the actual rate strings for min and max numeric values
            pl.struct(["numeric_si_rate", si_rate_col])
            .filter(pl.col("numeric_si_rate") == pl.col("numeric_si_rate").min())
            .first()
            .alias("struct_min"),
            pl.struct(["numeric_si_rate", si_rate_col])
            .filter(pl.col("numeric_si_rate") == pl.col("numeric_si_rate").max())
            .first()
            .alias("struct_max"),
        )
        .with_columns(
            (pl.col("max_rate") - pl.col("min_rate")).alias("rate_diff"),
            # NEW: Extract the actual rate strings with units
            pl.col("struct_min").struct.field(si_rate_col).alias("min_rate_with_units"),
            pl.col("struct_max").struct.field(si_rate_col).alias("max_rate_with_units"),
        )
    )

    # A group is variable if it has >1 unique rate string AND the numeric difference > epsilon.
    variable_groups = validation_summary.filter(
        (pl.col("n_unique_rates") > 1) & (pl.col("rate_diff") > config.RATE_EPSILON)
    )

    # Generate the representative "standard rate" for every group by taking the first one found.
    rate_descriptors = df.group_by(grouping_cols).agg(pl.col(si_rate_col).first().alias("first_si_rate"))

    if variable_groups.height == 0:
        logger.info("Rate consistency validation passed.")
        return {
            "is_consistent": True, "variable_groups_df": None, "rate_descriptors_df": rate_descriptors,
            "message": "All rates are consistent within their groups.", "unparsable_rate_count": unparsable_count
        }
    else:
        logger.warning(f"Rate consistency validation failed. Found {variable_groups.height} groups with variable rates.")
        message = f"Detected {variable_groups.height} groups with inconsistent rates."
        # Prepare the QA report for export with units included
        qa_df = variable_groups.select(
            grouping_cols + 
            ["n_unique_rates", "min_rate_with_units", "max_rate_with_units", "rate_diff", "example_rate"]
        ).sort(grouping_cols)
        
        return {
            "is_consistent": False, "variable_groups_df": qa_df, "rate_descriptors_df": rate_descriptors,
            "message": message, "unparsable_rate_count": unparsable_count
        }


def aggregate_data(
    df: pl.DataFrame, 
    grouping_cols: List[str], 
    start_date_col: str, 
    driver_col: str,
    date_trunc_unit: Optional[str] = None,
    si_rate_col: Optional[str] = None
) -> Tuple[pl.DataFrame, Optional[Dict[str, Any]]]:
    """
    Perform time-series aggregation with optional validation for rate consistency.

    This function orchestrates the entire backend aggregation process.

    Aggregation and Rate Handling Logic:
    1.  **Preprocessing**: The data is cleaned, dates are parsed, and the driver column
        is cast to a numeric type. If an `si_rate_col` is provided, it is passed
        through this stage untouched.
    2.  **Rate Validation**: If `si_rate_col` is present, the `_validate_rate_consistency`
        function is called. This checks if rates are constant within each group.
        The result of this check (a dictionary) determines if the process should
        warn the user or block execution.
    3.  **Pivoting**: The driver data is pivoted to create a wide table with monthly
        columns, summing the driver values for each group.
    4.  **ID Generation**: A unique composite ID is created for each group.
    5.  **Rate Join**: The "standard rate" for each group (as determined by the
        validation step) is joined back to the pivoted table as a single descriptive
        column. This ensures the rate is always visible for context and auditing.

    Args:
        df: Input DataFrame.
        grouping_cols: Column names to group by.
        start_date_col: Column containing start dates.
        driver_col: Column containing numeric driver values to sum.
        date_trunc_unit: Time unit for date truncation (e.g., "1mo").
        si_rate_col: Optional name of the SI rate column to validate and include.

    Returns:
        A tuple containing:
        - pl.DataFrame: The final pivoted DataFrame.
        - Optional[Dict[str, Any]]: A dictionary with validation results if a rate
          column was provided, otherwise None.
    """
    validation_result = None
    try:
        trunc_unit = date_trunc_unit or config.DATE_TRUNC_UNIT
        logger.info(f"Starting aggregation. Grouping by: {grouping_cols}, Truncating to: {trunc_unit}")

        _validate_columns(df, grouping_cols, start_date_col, driver_col)

        # Preprocess data, passing through the rate column if it exists.
        df_transformed = _preprocess_dataframe(
            df, grouping_cols, start_date_col, driver_col, trunc_unit,
            passthrough_cols=[si_rate_col] if si_rate_col else None
        )

        if df_transformed.height == 0:
            logger.warning("No valid data after preprocessing. Returning empty DataFrame.")
            empty_df = pl.DataFrame({col: [] for col in grouping_cols})
            final_df = _generate_id_column(empty_df, grouping_cols)
            return final_df, None
            
        # Validate rate consistency if a rate column is specified.
        rate_descriptors_df = None
        if si_rate_col and si_rate_col in df_transformed.columns:
            validation_result = _validate_rate_consistency(df_transformed, grouping_cols, si_rate_col)
            rate_descriptors_df = validation_result.get("rate_descriptors_df")

        # Pivot the driver data to monthly columns.
        pivoted = _pivot_monthly(df_transformed, grouping_cols)
        
        # Filter to only include columns that match YYYY-MM format.
        # This prevents rate columns or other metadata from being treated as temporal data.
        month_cols = sorted([col for col in pivoted.columns if _is_month_column(col)])
        
        if not month_cols:
            logger.warning("No month columns generated after pivot. Returning DataFrame with grouping columns only.")
            result = pivoted.select(grouping_cols) if pivoted.height > 0 else pl.DataFrame({c:[] for c in grouping_cols})
            final_df = _generate_id_column(result, grouping_cols)
            return final_df, validation_result

        pivoted = pivoted.select(grouping_cols + month_cols).fill_null(0)
        
        # Generate the composite ID column.
        pivoted_with_id = _generate_id_column(pivoted, grouping_cols)

        # Join the representative rate back to the pivoted data.
        # The rate is positioned after grouping identifiers but before temporal data
        # to maintain logical column ordering for downstream operations.
        final_df = pivoted_with_id
        if rate_descriptors_df is not None:
            final_df = pivoted_with_id.join(rate_descriptors_df, on=grouping_cols, how="left")
            final_df = final_df.rename({"first_si_rate": si_rate_col})
            final_df = final_df.select(["ID"] + grouping_cols + [si_rate_col] + month_cols)

        logger.info(f"Aggregation complete. Result shape: {final_df.shape}.")
        return final_df, validation_result

    except Exception as e:
        logger.error(f"An error occurred during data aggregation: {e}")
        raise