# test_data_factory.py

import polars as pl
from datetime import date, datetime

def create_toy_dataframe() -> pl.DataFrame:
    """
    Generates a sample Polars DataFrame for testing the backend pipeline.
    
    This DataFrame replicates how Excel data typically appears when loaded:
    - Date columns are loaded as strings (Excel's default export format)
    - Some malformed date strings and nulls to test error handling
    - Numeric columns with mixed int/float values and nulls
    - Multiple regions and product lines for grouping tests
    - All dates are within 2025 for consistency
    
    **Note**: The data for "North" -> "Gadgets" is specifically crafted to produce
    a realistic ore production pattern over months (starting at 0 and then fluctuating).
    """
    data = {
        "Region": [
            # Data for the fluctuating plot (North, Gadgets)
            "North", "North", "North", "North", "North", "North",
            # Other data to keep the test robust
            "South", "South", "West", "West", "South", "North"
        ],
        "Product Line": [
            # Data for the fluctuating plot (North, Gadgets)
            "Gadgets", "Gadgets", "Gadgets", "Gadgets", "Gadgets", "Gadgets",
            # Other data
            "Widgets", "Gadgets", "Widgets", "Gadgets", "Widgets", "Widgets"
        ],
        "Transaction Date": [
            # Fluctuating monthly ore production for "North", "Gadgets" - all in 2025
            "2025-01-15",  # 0 ore (startup phase)
            "2025-02-15",  # ramp up
            "2025-03-15",  # dip
            "2025-04-15",  # recovery
            "2025-05-15",  # peak
            "2025-06-15",  # slight decline
            # Other data for robust testing - all in 2025
            "2025-02-25",
            "not a date",          # Malformed string (test error handling)
            "2025-07-30",
            "2025-07-12",
            "2025-03-18",
            None,                  # Null date (test null handling)
        ],
        "Sales Volume": [
            # Realistic ore production (fluctuates, never negative, doesn't reset to 0)
            0.0,    # Jan: no production yet
            50.0,   # Feb: ramp up
            40.0,   # Mar: dip
            60.0,   # Apr: recovery
            90.0,   # May: strong peak
            75.0,   # Jun: slight decline but still high
            # Other data
            55.0,
            120.0,  # This will be dropped due to "not a date"
            300.0,
            175.0,
            210.0,
            88.2,   # This will be dropped due to null date
        ]
    }
    
    # Use proper schema that matches typical Excel data loading
    schema = {
        "Region": pl.Utf8,
        "Product Line": pl.Utf8,
        "Transaction Date": pl.Utf8,
        "Sales Volume": pl.Float64
    }

    df = pl.DataFrame(data, schema=schema)
    return df


def create_toy_dataframe_with_datetime_column() -> pl.DataFrame:
    """
    Alternative test data where dates are already parsed as datetime objects.
    This tests the scenario where Excel data has been pre-processed.
    All dates are in 2025 for consistency.
    """
    data = {
        "Region": ["North", "North", "South", "South", "North", "West"],
        "Product Line": ["Gadgets", "Widgets", "Gadgets", "Widgets", "Gadgets", "Widgets"],
        "Transaction Date": [
            datetime(2025, 1, 15),
            datetime(2025, 1, 20),
            datetime(2025, 2, 10),
            datetime(2025, 2, 25),
            datetime(2025, 3, 5),
            datetime(2025, 7, 30),
        ],
        "Sales Volume": [100.0, 150.5, 200.0, 55.0, 80.0, 300.0]
    }
    
    schema = {
        "Region": pl.Utf8,
        "Product Line": pl.Utf8,
        "Transaction Date": pl.Datetime,  # Already datetime type
        "Sales Volume": pl.Float64
    }

    return pl.DataFrame(data, schema=schema)


def create_toy_dataframe_with_date_column() -> pl.DataFrame:
    """
    Alternative test data where dates are Date objects (not Datetime).
    This tests the Date -> Datetime conversion path.
    All dates are in 2025 for consistency.
    """
    data = {
        "Region": ["North", "South", "West", "North"],
        "Product Line": ["Gadgets", "Widgets", "Gadgets", "Widgets"],
        "Transaction Date": [
            date(2025, 1, 15),
            date(2025, 2, 10),
            date(2025, 3, 5),
            date(2025, 1, 20),
        ],
        "Sales Volume": [100.0, 200.0, 175.0, 150.5]
    }
    
    schema = {
        "Region": pl.Utf8,
        "Product Line": pl.Utf8,
        "Transaction Date": pl.Date,  # Date type (not Datetime)
        "Sales Volume": pl.Float64
    }

    return pl.DataFrame(data, schema=schema)