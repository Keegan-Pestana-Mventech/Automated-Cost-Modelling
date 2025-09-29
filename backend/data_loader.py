import polars as pl


def load_excel_with_fallback(filepath: str, sheet_name: str) -> pl.DataFrame:
    """Load an Excel sheet with schema inference disabled to handle data type issues."""

    # Load with schema inference disabled to force all columns as strings
    df = pl.read_excel(filepath, sheet_name=sheet_name, infer_schema_length=0)
    print("Sheet loaded with schema inference disabled (all columns as strings)")
    return df
