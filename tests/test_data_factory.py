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


def create_df_with_consistent_rates() -> pl.DataFrame:
    """Creates a DataFrame where rates within each group are consistent."""
    data = {
        "Location": ["Pit A", "Pit A", "Pit B", "Pit B", "Pit B"],
        "Material": ["Ore", "Ore", "Waste", "Waste", "Ore"],
        "Start Date": ["2025-01-10", "2025-02-10", "2025-01-15", "2025-02-15", "2025-01-20"],
        "End Date": ["2025-01-31", "2025-02-28", "2025-01-31", "2025-02-28", "2025-01-31"],
        "Driver": [100, 150, 500, 600, 200],
        "Rate": ["10m/d", "10m/d", "50t/w", "50t/w", "25t/w"]
    }
    return pl.DataFrame(data)

def create_df_with_variable_rates() -> pl.DataFrame:
    """Creates a DataFrame where at least one group has variable rates."""
    data = {
        "Location": ["Pit A", "Pit A", "Pit B", "Pit B", "Pit B"],
        "Material": ["Ore", "Ore", "Waste", "Waste", "Ore"],
        "Start Date": ["2025-01-10", "2025-02-10", "2025-01-15", "2025-02-15", "2025-01-20"],
        "End Date": ["2025-01-31", "2025-02-28", "2025-01-31", "2025-02-28", "2025-01-31"],
        "Driver": [100, 150, 500, 600, 200],
        "Rate": ["10m/d", "12m/d", "50t/w", "50t/w", "25t/w"]  # Pit A, Ore is variable
    }
    return pl.DataFrame(data)

def create_df_with_unparsable_rates() -> pl.DataFrame:
    """Creates a DataFrame with a mix of valid, invalid, and consistent rates."""
    data = {
        "Location": ["Pit A", "Pit A", "Pit A", "Pit B", "Pit B"],
        "Material": ["Ore", "Ore", "Ore", "Waste", "Waste"],
        "Start Date": ["2025-01-10", "2025-02-10", "2025-03-10", "2025-01-15", "2025-02-15"],
        "End Date": ["2025-01-31", "2025-02-28", "2025-03-31", "2025-01-31", "2025-02-28"],
        "Driver": [100, 150, 120, 500, 600],
        "Rate": ["10m/d", "N/A", "10m/d", "50t/w", "50t/w"] # Pit A has consistent parsable rates, but one unparsable
    }
    return pl.DataFrame(data)

def create_df_for_epsilon_check() -> pl.DataFrame:
    """
    Creates a DataFrame to test the RATE_EPSILON configuration.
    
    The epsilon is set to 0.01 in config.py.
    After conversion to monthly SI units (using 30.44 days/month multiplier):
    - Pit C: 1.5m/d → 45.66 meter/mo (both entries identical)
      Difference: 0.0 (consistent rates, should be CONSISTENT)
    - Pit D: 1.5m/d → 45.66 meter/mo and 1.502m/d → 45.72 meter/mo  
      Difference: 0.06 (well above epsilon of 0.01, should be VARIABLE)
    """
    data = {
        "Location": ["Pit C", "Pit C", "Pit D", "Pit D"],
        "Material": ["Gold", "Gold", "Silver", "Silver"],
        "Start Date": ["2025-01-10", "2025-02-10", "2025-01-15", "2025-02-15"],
        "End Date": ["2025-01-31", "2025-02-28", "2025-01-31", "2025-02-28"],
        "Driver": [10, 12, 50, 60],
        # Pit C has identical rates (truly consistent)
        # Pit D has rates that differ by more than epsilon after conversion
        # 1.5m/d * 30.44 = 45.66 meter/mo
        # 1.502m/d * 30.44 = 45.72 meter/mo (diff = 0.06, clearly above epsilon)
        "Rate": ["1.5m/d", "1.5m/d", "1.5m/d", "1.502m/d"]
    }
    return pl.DataFrame(data)