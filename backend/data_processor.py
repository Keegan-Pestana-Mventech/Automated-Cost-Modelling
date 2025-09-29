import polars as pl
from typing import List


def aggregate_data(
    df: pl.DataFrame, grouping_cols: List[str], start_date_col: str, driver_col: str
) -> pl.DataFrame:
    """
    Performs time-series aggregation on the input DataFrame, pivoting by month,
    and adding a 'Total' column and a 'GRAND TOTAL' row.

    Args:
        df: The DataFrame containing the data to aggregate.
        grouping_cols: A list of column names to group by (e.g., location, activity).
        start_date_col: The name of the column containing the start date (e.g., 'start_date').
        driver_col: The name of the column containing the numeric driver values to sum (e.g., 'value').

    Returns:
        A Polars DataFrame pivoted by month with a 'Total' column and a 'GRAND TOTAL' row.
        Returns an empty DataFrame if no valid data is found after transformations.

    Raises:
        ValueError: If any required columns are missing from the input DataFrame.
        Exception: For other unexpected errors during processing.
    """
    # Input Validation
    required_cols = set(grouping_cols + [start_date_col, driver_col])
    available_cols = set(df.columns)

    if not required_cols.issubset(available_cols):
        missing = required_cols - available_cols
        raise ValueError(
            f"Missing required columns in DataFrame: {', '.join(missing)}. "
            f"Available columns: {', '.join(available_cols)}"
        )

    print(f"Starting aggregation. Grouping by: {grouping_cols}")

    try:
        # Select relevant columns and transform them using lazy evaluation
        # Convert date string to datetime, cast driver to float, handle nulls,
        # and truncate dates to the first day of the month for pivoting.
        df_transformed = (
            df.lazy()  # Start lazy evaluation for performance
            .select(grouping_cols + [start_date_col, driver_col])
            .with_columns(
                pl.col(start_date_col)
                .str.to_datetime(strict=False)
                .alias("parsed_date"),
                pl.col(driver_col)
                .cast(pl.Float64, strict=False)
                .fill_null(0)
                .alias("numeric_driver"),
            )
            .filter(
                pl.col("parsed_date").is_not_null()
            )  # Remove rows with invalid dates
            .with_columns(
                pl.col("parsed_date").dt.truncate("1mo").alias("month_period")
            )
            .collect()  # Collect the results after all transformations
        )

        # Early exit if no valid data remains
        if df_transformed.height == 0:
            print(
                "Warning: No valid data found after date parsing and filtering. Returning empty DataFrame."
            )
            # Create an empty DataFrame with expected columns for consistency
            empty_cols = grouping_cols + ["Total"]
            return pl.DataFrame({col: [] for col in empty_cols})

        # 2. Perform the pivot operation to aggregate driver values by month
        # This creates new columns for each unique month_period.
        pivoted_df = df_transformed.pivot(
            values="numeric_driver",
            index=grouping_cols,
            on="month_period",
            aggregate_function="sum",
        ).sort(grouping_cols)  # Sort by grouping columns for consistent output

        # 3. Sort the month columns chronologically and select final columns
        # Identify columns that represent months (not grouping columns)
        month_cols = sorted(
            [col for col in pivoted_df.columns if col not in grouping_cols]
        )

        if not month_cols:
            print(
                "Warning: No month columns generated after pivot. This might indicate no data or an issue with month_period creation."
            )
            # Return the pivoted_df as is, potentially with just grouping_cols
            return pivoted_df.select(grouping_cols)

        # Ensure all month columns are present, filling with 0 if a group had no data for a month
        # This is important if `pivot` doesn't create all possible month columns for all groups
        final_df = pivoted_df.select(grouping_cols + month_cols).fill_null(0)

        # 4. Add a horizontal total for each row (sum across month columns)
        final_df = final_df.with_columns(Total=pl.sum_horizontal(month_cols))

        # 5. Create and append the "GRAND TOTAL" summary row
        # Calculate sums for all numeric columns (month columns + 'Total')
        numeric_totals = final_df.select(month_cols + ["Total"]).sum()

        # Construct the grand total row data
        grand_total_data = {}
        # The first grouping column gets the "GRAND TOTAL" label
        grand_total_data[grouping_cols[0]] = "GRAND TOTAL"
        # Subsequent grouping columns are left blank or with a placeholder
        for col in grouping_cols[1:]:
            grand_total_data[col] = ""

        # Add the calculated numeric totals
        for col in month_cols + ["Total"]:
            grand_total_data[col] = numeric_totals[col][
                0
            ]  # .sum() returns a DataFrame, take the first element

        # Create a DataFrame for the total row and concatenate
        total_row = pl.DataFrame([grand_total_data])
        final_df = pl.concat([final_df, total_row], how="vertical")

        return final_df

    except Exception as e:
        print(f"An error occurred during data aggregation: {e}")
        raise  # Re-raise the exception after logging for upstream handling
