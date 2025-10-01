# data_filter.py

import polars as pl
import logging
from typing import List, Dict, Any
from datetime import datetime
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)


def smooth_data_with_stockpile(
    df: pl.DataFrame, 
    grouping_cols: List[str], 
    driver_threshold: float
) -> pl.DataFrame:
    """
    Apply stockpile-based smoothing to time series data.
    
    Logic:
    - Each month has a reported_quantity (actual production)
    - actual_quantity is what we can process given the threshold capacity
    - If reported > threshold: excess goes to stockpile
    - If reported < threshold: draw from stockpile to try to reach threshold
    - After data ends, continue depleting stockpile at threshold rate
    
    Args:
        df: Pivoted DataFrame from data_processor.aggregate_data
        grouping_cols: Column names used for grouping
        driver_threshold: Maximum capacity per month
        
    Returns:
        DataFrame with smoothed values
    """
    try:
        logger.info(f"Starting stockpile smoothing with threshold: {driver_threshold}")
        
        if df.height == 0:
            logger.warning("Empty DataFrame provided for smoothing")
            return df
        
        # Get month columns (sorted chronologically), excluding grouping and ID columns
        exclude_cols = grouping_cols + ["ID"]
        month_cols = sorted([col for col in df.columns if col not in exclude_cols])
        
        if not month_cols:
            logger.warning("No month columns found")
            return df
        
        # Process each group separately
        all_results = []
        
        for row_idx in range(df.height):
            # Get grouping values for this row
            group_values = {col: df[col][row_idx] for col in grouping_cols}
            
            # Get the time series for this group - include ALL months from the original data
            monthly_data = []
            has_any_data = False
            
            # Get reported quantities for all months (Driver values)
            for month in month_cols:
                value = df[month][row_idx]
                if value is None:
                    value = 0.0
                else:
                    value = float(value)
                    if value > 0:
                        has_any_data = True
                
                monthly_data.append({
                    'month': month,
                    'reported_quantity': value
                })
            
            # If a group has no data at all, skip it
            if not has_any_data:
                continue

            # Apply stockpile logic to the full series
            smoothed_data = _apply_stockpile_smoothing(monthly_data, driver_threshold)
            
            # Add grouping columns back
            for entry in smoothed_data:
                entry.update(group_values)
            
            all_results.extend(smoothed_data)
        
        if not all_results:
             logger.warning("No data produced after smoothing.")
             return pl.DataFrame({col: [] for col in grouping_cols})

        # Convert to DataFrame
        result_df = pl.DataFrame(all_results)
        
        # Pivot back to wide format
        pivoted = result_df.pivot(
            values="actual_quantity",
            index=grouping_cols,
            on="month",
            aggregate_function="first"
        )
        
        # Get all unique months (including extended ones) and sort
        all_months = sorted([col for col in pivoted.columns if col not in grouping_cols])
        
        # Reorder columns: grouping_cols + sorted months
        pivoted = pivoted.select(grouping_cols + all_months).fill_null(0)
        
        logger.info(f"Stockpile smoothing complete. Result shape: {pivoted.shape}")
        return pivoted
        
    except Exception as e:
        logger.error(f"Error during stockpile smoothing: {e}")
        raise


def _apply_stockpile_smoothing(
    monthly_data: List[Dict[str, Any]], 
    threshold: float
) -> List[Dict[str, Any]]:
    """
    Apply stockpile smoothing to a time series.
    
    Args:
        monthly_data: List of dicts with 'month' and 'reported_quantity' for ALL months.
        threshold: Capacity constraint
        
    Returns:
        List of dicts with smoothed values and extended months if needed
    """
    if not monthly_data:
        return []

    # Convert to DataFrame for easier processing
    source_df = pl.DataFrame(monthly_data).with_columns(
        pl.col("month").str.to_date("%Y-%m").alias("date")
    ).sort("date")

    # Apply stockpile logic
    results = []
    stockpile = 0.0
    
    for row in source_df.iter_rows(named=True):
        month_str = row['date'].strftime("%Y-%m")
        reported = row['reported_quantity']
        
        stockpile_start = stockpile
        
        # Calculate how much we can actually process this month
        if reported >= threshold:
            # Production meets or exceeds capacity - store excess
            actual_quantity = threshold
            excess = reported - threshold
            stockpile_end = stockpile_start + excess
        else:
            # Production is below capacity - try to draw from stockpile
            shortfall = threshold - reported
            draw_from_stockpile = min(shortfall, stockpile_start)
            actual_quantity = reported + draw_from_stockpile
            stockpile_end = stockpile_start - draw_from_stockpile
        
        results.append({
            'month': month_str,
            'reported_quantity': reported,
            'actual_quantity': actual_quantity,
        })
        
        stockpile = stockpile_end
    
    # Deplete remaining stockpile after all data has been processed
    if stockpile > 0:
        last_month_date = source_df["date"].max()
        current_date = last_month_date
        
        iteration_count = 0
        # Limit iterations to prevent infinite loops
        while stockpile > 0 and iteration_count < 120: 
            # Move to the next month
            current_date = current_date + relativedelta(months=1)
            next_month = current_date.strftime("%Y-%m")
            
            # Deplete from stockpile up to the threshold
            draw_amount = min(stockpile, threshold)
            
            results.append({
                'month': next_month,
                'reported_quantity': 0.0, # No new production
                'actual_quantity': draw_amount,
            })
            
            stockpile -= draw_amount
            iteration_count += 1
        
        if iteration_count >= 120:
            logger.warning("Stockpile depletion exceeded 10-year iteration limit.")
    
    return results