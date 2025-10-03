import polars as pl
import logging
import re
from typing import Optional, Tuple
import pint

logger = logging.getLogger(__name__)

# Initialize pint's unit registry
ureg = pint.UnitRegistry()

# Define time period conversions to monthly basis
TIME_TO_MONTH_MULTIPLIERS = {
    'week': 4.33,  # Average weeks per month
    'w': 4.33,
    'day': 30.44,  # Average days per month
    'd': 30.44,
    'year': 1/12,
    'y': 1/12,
    'yr': 1/12,
    'month': 1,
    'mo': 1,
    'm': 1,  # This could be ambiguous with meters, but in rate context it's month
}


def parse_rate_string(rate_str: str) -> Optional[Tuple[float, str, str]]:
    """
    Parse a rate string like "52ftp/w" or "60m/mo" into components.
    
    Args:
        rate_str: String containing rate with unit and time period
        
    Returns:
        Tuple of (value, unit, time_period) or None if parsing fails
        
    Example:
        "52ftp/w" -> (52.0, "ftp", "w")
        "60m/mo" -> (60.0, "m", "mo")
        "10.5 liters/week" -> (10.5, "liters", "week")
    """
    if not isinstance(rate_str, str):
        return None
    
    rate_str = rate_str.strip()
    
    # Pattern to match: number + unit + "/" + time period
    # Supports spaces and various formats
    pattern = r'^([\d.]+)\s*([a-zA-Z]+)\s*/\s*([a-zA-Z]+)$'
    match = re.match(pattern, rate_str)
    
    if not match:
        logger.warning(f"Could not parse rate string: '{rate_str}'")
        return None
    
    value_str, unit, time_period = match.groups()
    
    try:
        value = float(value_str)
        return (value, unit, time_period)
    except ValueError:
        logger.warning(f"Could not convert value to float: '{value_str}'")
        return None


def convert_unit_to_si(value: float, unit_str: str) -> Tuple[float, str]:
    """
    Convert a value with a given unit to its SI equivalent using pint.
    
    Args:
        value: Numeric value
        unit_str: Unit string (e.g., "ft", "gal", "liter")
        
    Returns:
        Tuple of (converted_value, si_unit_string)
        
    Example:
        (10, "ft") -> (3.048, "meter")
        (5, "gallon") -> (18.927, "liter")
    """
    # Handle common abbreviations that pint might not recognize
    unit_aliases = {
        # Length units
        'ftp': 'foot',
        'ft': 'foot',
        'm': 'meter',  # When not in time context
        'meters': 'meter',
        'metres': 'meter',
        'dm': 'decimeter',
        
        # Volume units
        'gal': 'gallon',
        'l': 'liter',
        'liters': 'liter',
        'litres': 'liter',
        
        # Mass units
        'wmt': 'metric_ton',  # Wet metric ton
        'dmt': 'metric_ton',  # Dry metric ton (same base unit, different context)
        'mt': 'metric_ton',
        'tonne': 'metric_ton',
        'tonnes': 'metric_ton',
        't': 'metric_ton',
    }
    
    normalized_unit = unit_aliases.get(unit_str.lower(), unit_str)
    
    try:
        # Create a quantity with the given value and unit
        quantity = value * ureg(normalized_unit)
        
        # Convert to appropriate SI unit based on dimensionality
        if quantity.dimensionality == ureg.meter.dimensionality:
            # Length - convert to meters
            si_quantity = quantity.to('meter')
        elif quantity.dimensionality == ureg.liter.dimensionality:
            # Volume - prefer liters over cubic meters for readability
            si_quantity = quantity.to('liter')
        elif quantity.dimensionality == ureg.kilogram.dimensionality:
            # Mass - convert to kilograms (metric tons -> kg)
            si_quantity = quantity.to('kilogram')
        else:
            # For other units, use base units
            si_quantity = quantity.to_base_units()
        
        # Clean up unit string formatting
        unit_str_clean = str(si_quantity.units)
        unit_str_clean = unit_str_clean.replace(' ** 3', '³')  # Clean up exponent notation
        unit_str_clean = unit_str_clean.replace(' ** 2', '²')
        
        return (si_quantity.magnitude, unit_str_clean)
        
    except (pint.UndefinedUnitError, pint.DimensionalityError) as e:
        logger.warning(f"Could not convert unit '{unit_str}': {e}. Returning original value.")
        return (value, unit_str)


def convert_rate_to_monthly_si(rate_str: str) -> str:
    """
    Convert a rate string to monthly SI units.
    
    Args:
        rate_str: Original rate string (e.g., "52ftp/w")
        
    Returns:
        Converted rate string (e.g., "67.36meter/mo")
        
    Process:
        1. Parse the rate string
        2. Convert unit to SI
        3. Convert time period to monthly basis
        4. Format as new rate string
    """
    # Parse the original rate
    parsed = parse_rate_string(rate_str)
    if not parsed:
        logger.warning(f"Returning original string due to parse failure: '{rate_str}'")
        return rate_str
    
    value, unit, time_period = parsed
    
    # Convert the unit to SI
    si_value, si_unit = convert_unit_to_si(value, unit)
    
    # Convert time period to monthly basis
    time_multiplier = TIME_TO_MONTH_MULTIPLIERS.get(time_period.lower())
    if time_multiplier is None:
        logger.warning(f"Unknown time period '{time_period}'. Assuming monthly.")
        time_multiplier = 1
    
    monthly_value = si_value * time_multiplier
    
    # Format the result
    # Round to 2 decimal places for readability
    result = f"{monthly_value:.2f}{si_unit}/mo"
    
    logger.info(f"Converted '{rate_str}' -> '{result}'")
    return result


def add_si_rate_column(
    df: pl.DataFrame,
    rate_column_name: str
) -> pl.DataFrame:
    """
    Add a new column with SI-converted rates to the dataframe.
    
    This function serves as the first step in the rate handling pipeline. It creates
    a standardized, comparable rate column (e.g., "SI Rate") that will be used
    downstream by `data_processor.py` to validate rate consistency.
    
    Args:
        df: Input DataFrame containing the rate column.
        rate_column_name: Name of the column containing rate strings.
        
    Returns:
        DataFrame with a new "SI {rate_column_name}" column added.
        
    Raises:
        ValueError: If the specified column doesn't exist.
    """
    if rate_column_name not in df.columns:
        raise ValueError(
            f"Column '{rate_column_name}' not found in DataFrame. "
            f"Available columns: {', '.join(df.columns)}"
        )
    
    logger.info(f"Converting rates in column '{rate_column_name}' to monthly SI units")
    
    # Apply the conversion to each rate string using map_elements for string operations.
    si_column_name = f"SI {rate_column_name}"
    
    df_with_si = df.with_columns(
        pl.col(rate_column_name)
        .map_elements(convert_rate_to_monthly_si, return_dtype=pl.String)
        .alias(si_column_name)
    )
    
    logger.info(f"Successfully added column '{si_column_name}'")
    return df_with_si