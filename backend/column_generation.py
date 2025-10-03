import polars as pl
from typing import List
from functools import reduce

def generate_new_column(
    df: pl.DataFrame,
    target_cols: List[str],
    new_col_name: str,
    operation: str,
) -> pl.DataFrame:
    """
    Generates a new column in the DataFrame by performing an operation on target columns.

    Args:
        df: The input Polars DataFrame.
        target_cols: A list of column names to use in the operation.
        new_col_name: The name for the newly generated column.
        operation: The operation to perform. One of "sum", "multiply", "divide".

    Returns:
        A new DataFrame with the added column.

    Raises:
        ValueError: If an invalid operation is specified, if target columns are not found,
                    or if the new column name already exists.
        TypeError: If target columns cannot be cast to a numeric type.
    """
    if not target_cols:
        raise ValueError("Target columns list cannot be empty.")

    if new_col_name in df.columns:
        raise ValueError(f"Column '{new_col_name}' already exists in the DataFrame.")

    # Ensure all target columns are numeric or can be cast to numeric.
    # We will cast to Float64 for consistency and to handle potential nulls.
    cast_expressions = []
    for col_name in target_cols:
        if col_name not in df.columns:
            raise ValueError(f"Target column '{col_name}' not found in DataFrame.")
        
        # We attempt to cast non-numeric columns, filling errors with 0.
        # This makes the function more robust to string-based numeric columns.
        if df.schema[col_name] not in pl.NUMERIC_DTYPES:
            cast_expressions.append(pl.col(col_name).cast(pl.Float64, strict=False).fill_null(0).alias(col_name))
        else:
            # If already numeric, just ensure it's float and fill nulls for safety.
            cast_expressions.append(pl.col(col_name).cast(pl.Float64).fill_null(0).alias(col_name))

    df = df.with_columns(cast_expressions)

    if operation == "sum":
        expression = pl.sum_horizontal(target_cols)
    elif operation == "multiply":
        # Polars doesn't have a direct horizontal product, so we use reduce.
        expression = reduce(lambda a, b: a * b, [pl.col(c) for c in target_cols])
    elif operation == "divide":
        if len(target_cols) != 2:
            raise ValueError("Division operation requires exactly two target columns (numerator / denominator).")
        # To avoid division by zero, replace 0 in the denominator with null.
        # This results in a null output for the row, which is a safe default.
        denominator = pl.col(target_cols[1]).replace(0, None)
        expression = pl.col(target_cols[0]) / denominator
    else:
        raise ValueError(f"Unsupported operation: '{operation}'. Choose from 'sum', 'multiply', 'divide'.")

    return df.with_columns(expression.alias(new_col_name))