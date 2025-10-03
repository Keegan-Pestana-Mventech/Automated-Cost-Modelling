import sys
from pathlib import Path
import polars as pl
import numpy as np # Added for np.isclose assertions

# Add the project root to the Python path to allow importing from backend and tests
project_root = Path(__file__).resolve().parents[1]
sys.path.append(str(project_root))

from tests.test_data_factory import (
    create_toy_dataframe, 
    create_toy_dataframe_with_datetime_column,
    create_toy_dataframe_with_date_column,
    create_df_with_consistent_rates,
    create_df_with_variable_rates,
    create_df_with_unparsable_rates,
    create_df_for_epsilon_check
)
from backend import data_processor, plot_generator, utils, data_filter, unit_converter, column_generation

# Ensure the output directory exists for plot saving
import config
config.OUTPUT_DIRECTORY.mkdir(exist_ok=True)
(config.OUTPUT_DIRECTORY / config.PLOT_OUTPUT_SUBDIR).mkdir(exist_ok=True)


def test_aggregation_string_dates():
    """
    Tests aggregation with string date columns (most common Excel scenario).
    """
    print("\n" + "="*40)
    print("  TESTING STRING DATE AGGREGATION  ")
    print("="*40 + "\n")
    
    # 1. Get toy data with string dates
    source_df = create_toy_dataframe()
    print("--- 1. Initial Source DataFrame (String Dates) ---")
    print(source_df)
    print(f"Transaction Date dtype: {source_df.schema['Transaction Date']}")
    print("\n")

    # 2. Define aggregation parameters
    grouping_cols = ["Region", "Product Line"]
    start_date_col = "Transaction Date"
    driver_col = "Sales Volume"
    
    # 3. Run the aggregation function
    print("--- 2. Running data_processor.aggregate_data... ---\n")
    # NOTE: Updated to handle the new return signature (df, validation_results)
    aggregated_df, _ = data_processor.aggregate_data(
        df=source_df,
        grouping_cols=grouping_cols,
        start_date_col=start_date_col,
        driver_col=driver_col,
        date_trunc_unit="1mo"
    )
    
    # 4. Inspect the final output
    print("--- 3. Final Aggregated DataFrame ---")
    inspector = utils.DataFrameInspector()
    inspector.inspect_dataframe(aggregated_df, "FINAL AGGREGATED DATA")
    
    print("\n[SUCCESS] String date aggregation test completed.\n")
    
    return aggregated_df


def test_aggregation_datetime_column():
    """
    Tests aggregation with datetime columns (pre-processed Excel data).
    """
    print("\n" + "="*40)
    print("  TESTING DATETIME COLUMN AGGREGATION  ")
    print("="*40 + "\n")
    
    source_df = create_toy_dataframe_with_datetime_column()
    print("--- 1. Source DataFrame (Datetime Column) ---")
    print(source_df)
    print(f"Transaction Date dtype: {source_df.schema['Transaction Date']}")
    print("\n")

    grouping_cols = ["Region", "Product Line"]
    start_date_col = "Transaction Date"
    driver_col = "Sales Volume"
    
    aggregated_df, _ = data_processor.aggregate_data(
        df=source_df,
        grouping_cols=grouping_cols,
        start_date_col=start_date_col,
        driver_col=driver_col,
        date_trunc_unit="1mo"
    )
    
    print("--- 2. Final Aggregated DataFrame ---")
    print(aggregated_df)
    print("\n[SUCCESS] Datetime column aggregation test completed.\n")
    
    return aggregated_df


def test_aggregation_date_column():
    """
    Tests aggregation with Date columns (Date objects, not Datetime).
    """
    print("\n" + "="*40)
    print("  TESTING DATE COLUMN AGGREGATION  ")
    print("="*40 + "\n")
    
    source_df = create_toy_dataframe_with_date_column()
    print("--- 1. Source DataFrame (Date Column) ---")
    print(source_df)
    print(f"Transaction Date dtype: {source_df.schema['Transaction Date']}")
    print("\n")

    grouping_cols = ["Region", "Product Line"]
    start_date_col = "Transaction Date"
    driver_col = "Sales Volume"
    
    aggregated_df, _ = data_processor.aggregate_data(
        df=source_df,
        grouping_cols=grouping_cols,
        start_date_col=start_date_col,
        driver_col=driver_col,
        date_trunc_unit="1mo"
    )
    
    print("--- 2. Final Aggregated DataFrame ---")
    print(aggregated_df)
    print("\n[SUCCESS] Date column aggregation test completed.\n")
    
    return aggregated_df


def test_data_filtering_and_comparison_plots():
    """
    Tests the data filtering with stockpile logic, generates comparison plots,
    and programmatically asserts the correctness of the smoothing.
    """
    print("\n" + "="*50)
    print("  TESTING DATA FILTERING WITH STOCKPILE LOGIC  ")
    print("="*50 + "\n")
    
    # 1. Get and aggregate the source data
    source_df = create_toy_dataframe()
    print("--- 1. Source Data ---")
    print(source_df)
    print("\n")
    
    grouping_cols = ["Region", "Product Line"]
    start_date_col = "Transaction Date"
    driver_col = "Sales Volume"
    
    # Aggregate the data first
    aggregated_df, _ = data_processor.aggregate_data(
        df=source_df,
        grouping_cols=grouping_cols,
        start_date_col=start_date_col,
        driver_col=driver_col,
        date_trunc_unit="1mo"
    )
    
    print("--- 2. Aggregated Data (Before Smoothing) ---")
    print(aggregated_df)
    print("\n")
    
    # 2. Apply stockpile smoothing
    driver_threshold = 50.0  # Set capacity threshold
    print(f"--- 3. Applying Stockpile Smoothing (Threshold: {driver_threshold}) ---")
    
    smoothed_df = data_filter.smooth_data_with_stockpile(
        df=aggregated_df,
        grouping_cols=grouping_cols,
        driver_threshold=driver_threshold
    )
    
    print("--- 4. Smoothed Data (After Stockpile Logic) ---")
    print(smoothed_df)
    print("\n")

    # 5. Programmatic Verification for "North | Gadgets"
    print("--- 5. Verifying Smoothing Logic for 'North | Gadgets' ---")
    
    # Isolate original and smoothed data for the specific group
    original_series = aggregated_df.filter(
        (pl.col("Region") == "North") & (pl.col("Product Line") == "Gadgets")
    )
    smoothed_series = smoothed_df.filter(
        (pl.col("Region") == "North") & (pl.col("Product Line") == "Gadgets")
    )

    assert smoothed_series.height == 1, "Should find exactly one smoothed row for North/Gadgets"

    # Define expected values based on our manual trace
    expected_values = {
        "2025-01": 0.0,    # rep=0, stock=0 -> act=0
        "2025-02": 50.0,   # rep=50, stock=0 -> act=50
        "2025-03": 40.0,   # rep=40, stock=0 -> act=40
        "2025-04": 50.0,   # rep=60, stock=10 -> act=50
        "2025-05": 50.0,   # rep=90, stock=10+40=50 -> act=50
        "2025-06": 50.0,   # rep=75, stock=50+25=75 -> act=50
        "2025-07": 50.0,   # rep=0, stock=75 -> draw=50, act=50, stock=25
        "2025-08": 25.0,   # depletion, stock=25 -> draw=25, act=25, stock=0
    }
   
    # Check each expected month's value
    for month, expected_val in expected_values.items():
        assert month in smoothed_series.columns, f"Expected month {month} is missing in smoothed data"
        actual_val = smoothed_series[month][0]
        print(f"Checking {month}: Expected={expected_val}, Actual={actual_val}")
        assert np.isclose(actual_val, expected_val), f"Mismatch in {month}!"

    if "2025-09" in smoothed_series.columns:
        assert smoothed_series["2025-09"][0] == 0.0, "Depletion should have stopped, 2025-09 should be 0"

    # The most important check: Conservation of mass. The total volume must be the same.
    exclude_cols = grouping_cols + ["ID"]
    original_month_cols = [c for c in original_series.columns if c not in exclude_cols]
    smoothed_month_cols = [c for c in smoothed_series.columns if c not in grouping_cols]

    original_total = original_series.select(pl.sum_horizontal(original_month_cols))[0,0]
    smoothed_total = smoothed_series.select(pl.sum_horizontal(smoothed_month_cols))[0,0]
    
    print(f"Original Total Volume: {original_total}")
    print(f"Smoothed Total Volume: {smoothed_total}")
    assert np.isclose(original_total, smoothed_total), "Total volume before and after smoothing must be equal!"

    print("\n[SUCCESS] Smoothing logic verified programmatically.\n")
    
    # 6. Generate comparison plots
    if aggregated_df.height > 0:
        entry_index = 0 # This corresponds to "North | Gadgets"
        label_parts = [str(aggregated_df[col][entry_index]) for col in grouping_cols]
        selected_label = " | ".join(label_parts)
        
        print(f"--- 6. Generating Comparison Plots for: '{selected_label}' ---\n")
        
        plot_generator.generate_comparison_plot(
            original_df=aggregated_df,
            smoothed_df=smoothed_df,
            entry_index=entry_index,
            selected_entry_label=selected_label,
            grouping_cols=grouping_cols,
            driver_col_name=driver_col,
            plot_settings=config.DEFAULT_PLOT_SETTINGS,
            output_filename=f"comparison_{selected_label.replace(' | ', '_')}.png"
        )
        
        print("\n--- 6. Comparison Complete ---")
        
    else:
        print("[SKIP] No data rows available for plotting.")
        
    print("\n[SUCCESS] Data filtering and comparison test completed.\n")
    return aggregated_df, smoothed_df


def test_unit_conversion():
    """
    Tests the unit conversion functionality for rate columns.
    """
    print("\n" + "="*40)
    print("  TESTING UNIT CONVERSION  ")
    print("="*40 + "\n")
    
    # Create a test DataFrame with rate data including the requested units
    test_data = {
        "Rates": ["52ftp/w", "60m/mo", "10gal/d", "100ft/yr", "25wmt/mo", "15dmt/mo", "200dm/d"]
    }
    test_df = pl.DataFrame(test_data)
    
    print("--- 1. Original DataFrame ---")
    print(test_df)
    print("\n")
    
    # Apply the unit conversion
    print("--- 2. Converting Rates to Monthly SI Units ---")
    converted_df = unit_converter.add_si_rate_column(test_df, "Rates")
    
    print("\n--- 3. DataFrame with SI Rates ---")
    print(converted_df)
    print("\n")
    
    # Verify the SI Rates column was added using the config alias
    si_col_name = config.RATE_COLUMN_ALIAS.format("Rates")
    assert si_col_name in converted_df.columns, f"{si_col_name} column not added"
    assert converted_df.height == test_df.height, "Row count changed unexpectedly"
    
    # Test individual conversions
    print("--- 4. Verifying Individual Conversions ---")
    
    # Test cases
    assert "meter/mo" in converted_df[si_col_name][0]
    assert "meter/mo" in converted_df[si_col_name][1]
    assert "liter/mo" in converted_df[si_col_name][2]
    assert "meter/mo" in converted_df[si_col_name][3]
    assert "kilogram/mo" in converted_df[si_col_name][4]
    assert "kilogram/mo" in converted_df[si_col_name][5]
    assert "meter/mo" in converted_df[si_col_name][6]
    print("[SUCCESS] All unit conversions are correct.")
    
    print("\n--- 5. Testing Edge Cases ---")
    edge_case_data = {"Rates": ["invalid_format", "50/", "abc/mo"]}
    edge_df = pl.DataFrame(edge_case_data)
    edge_converted = unit_converter.add_si_rate_column(edge_df, "Rates")
    
    for i, rate in enumerate(edge_df["Rates"]):
        converted = edge_converted[si_col_name][i]
        assert converted == rate, f"Invalid format '{rate}' should return original string"
    print("[SUCCESS] Edge cases handled correctly.")
    
    print("\n[SUCCESS] Unit conversion test completed.\n")
    return converted_df


def test_rate_validation_and_aggregation():
    """
    Tests the full pipeline including SI rate conversion, validation, and aggregation.
    """
    print("\n" + "="*50)
    print("  TESTING RATE VALIDATION & AGGREGATION  ")
    print("="*50 + "\n")

    grouping_cols = ["Location", "Material"]
    start_date_col = "Start Date"
    driver_col = "Driver"
    rate_col = "Rate"
    si_rate_col = config.RATE_COLUMN_ALIAS.format(rate_col)

    # --- Test Case 1: Consistent Rates ---
    print("--- 1. Testing with CONSISTENT rates ---")
    df_consistent = create_df_with_consistent_rates()
    df_consistent_si = unit_converter.add_si_rate_column(df_consistent, rate_col)
    
    agg_df_consistent, val_res_consistent = data_processor.aggregate_data(
        df=df_consistent_si, grouping_cols=grouping_cols, start_date_col=start_date_col,
        driver_col=driver_col, rate_col=rate_col, si_rate_col=si_rate_col
    )
    
    assert val_res_consistent['is_consistent'] is True
    assert val_res_consistent['variable_groups_df'] is None
    assert val_res_consistent['unparsable_rate_count'] == 0
    assert rate_col in agg_df_consistent.columns
    assert si_rate_col in agg_df_consistent.columns
    assert agg_df_consistent.height == 3
    print("[SUCCESS] Consistent rates handled correctly.\n")

    # --- Test Case 2: Variable Rates ---
    print("--- 2. Testing with VARIABLE rates ---")
    df_variable = create_df_with_variable_rates()
    df_variable_si = unit_converter.add_si_rate_column(df_variable, rate_col)
    
    agg_df_variable, val_res_variable = data_processor.aggregate_data(
        df=df_variable_si, grouping_cols=grouping_cols, start_date_col=start_date_col,
        driver_col=driver_col, rate_col=rate_col, si_rate_col=si_rate_col
    )

    assert val_res_variable['is_consistent'] is False
    assert val_res_variable['variable_groups_df'] is not None
    assert val_res_variable['variable_groups_df'].height == 1
    assert rate_col in agg_df_variable.columns
    assert si_rate_col in agg_df_variable.columns
    print("[SUCCESS] Variable rates detected correctly.\n")

    # --- Test Case 3: Unparsable Rates ---
    print("--- 3. Testing with UNPARSABLE rates ---")
    df_unparsable = create_df_with_unparsable_rates()
    df_unparsable_si = unit_converter.add_si_rate_column(df_unparsable, rate_col)
    
    agg_df_unparsable, val_res_unparsable = data_processor.aggregate_data(
        df=df_unparsable_si, grouping_cols=grouping_cols, start_date_col=start_date_col,
        driver_col=driver_col, rate_col=rate_col, si_rate_col=si_rate_col
    )
    
    assert val_res_unparsable['is_consistent'] is True 
    assert val_res_unparsable['unparsable_rate_count'] == 1
    assert rate_col in agg_df_unparsable.columns
    assert si_rate_col in agg_df_unparsable.columns
    print("[SUCCESS] Unparsable rates were correctly counted and ignored by validation.\n")

    # --- Test Case 4: Epsilon Check ---
    print("--- 4. Testing RATE_EPSILON check ---")
    df_epsilon = create_df_for_epsilon_check()
    df_epsilon_si = unit_converter.add_si_rate_column(df_epsilon, rate_col)

    agg_df_epsilon, val_res_epsilon = data_processor.aggregate_data(
        df=df_epsilon_si, grouping_cols=grouping_cols, start_date_col=start_date_col,
        driver_col=driver_col, rate_col=rate_col, si_rate_col=si_rate_col
    )

    assert val_res_epsilon['is_consistent'] is False
    assert val_res_epsilon['variable_groups_df'].height == 1
    variable_group = val_res_epsilon['variable_groups_df'][0, "Location"]
    assert variable_group == "Pit D"
    assert rate_col in agg_df_epsilon.columns
    assert si_rate_col in agg_df_epsilon.columns
    print(f"[SUCCESS] Epsilon check correctly identified variable group 'Pit D'.\n")

    print("[SUCCESS] All rate validation tests completed.\n")


def test_column_generation():
    """
    Tests the column generation functionality.
    """
    print("\n" + "="*40)
    print("  TESTING COLUMN GENERATION  ")
    print("="*40 + "\n")

    # 1. Create a base DataFrame
    data = {'A': [1, 2, 3], 'B': [10, 20, 30], 'C': [2, 4, 2], 'D_str': ["5", "10", "15"]}
    df = pl.DataFrame(data)
    print("--- 1. Original DataFrame ---\n", df, "\n")

    # 2. Test SUM operation
    print("--- 2. Testing SUM: A + B ---")
    df_sum = column_generation.generate_new_column(df, ['A', 'B'], 'Sum_A_B', 'sum')
    expected_sum = pl.Series("Sum_A_B", [11.0, 22.0, 33.0])
    assert df_sum['Sum_A_B'].equals(expected_sum)
    print("[SUCCESS] Sum operation is correct.\n")

    # 3. Test MULTIPLY operation
    print("--- 3. Testing MULTIPLY: A * C ---")
    df_mul = column_generation.generate_new_column(df, ['A', 'C'], 'Mul_A_C', 'multiply')
    expected_mul = pl.Series("Mul_A_C", [2.0, 8.0, 6.0])
    assert df_mul['Mul_A_C'].equals(expected_mul)
    print("[SUCCESS] Multiply operation is correct.\n")
    
    # 4. Test DIVIDE operation
    print("--- 4. Testing DIVIDE: B / C ---")
    df_div = column_generation.generate_new_column(df, ['B', 'C'], 'Div_B_C', 'divide')
    expected_div = pl.Series("Div_B_C", [5.0, 5.0, 15.0])
    assert df_div['Div_B_C'].equals(expected_div)
    print("[SUCCESS] Divide operation is correct.\n")
    
    # 5. Test with string casting
    print("--- 5. Testing SUM with string cast: A + D_str ---")
    df_cast = column_generation.generate_new_column(df, ['A', 'D_str'], 'Sum_A_Dstr', 'sum')
    expected_cast_sum = pl.Series("Sum_A_Dstr", [6.0, 12.0, 18.0])
    assert df_cast['Sum_A_Dstr'].equals(expected_cast_sum)
    print("[SUCCESS] Sum with string casting is correct.\n")

    print("[SUCCESS] Column generation test completed.\n")


if __name__ == "__main__":
    print("Starting comprehensive backend tests...")
    
    # Test Suite 1: Basic Aggregation
    print("\n" + "="*60)
    print("  RUNNING TEST SUITE 1: DATA AGGREGATION  ")
    print("="*60 + "\n")
    test_aggregation_string_dates()
    test_aggregation_datetime_column()
    test_aggregation_date_column()
    
    # Test Suite 2: Stockpile Smoothing & Plotting
    print("\n" + "="*60)
    print("  RUNNING TEST SUITE 2: STOCKPILE SMOOTHING & PLOTS  ")
    print("="*60 + "\n")
    test_data_filtering_and_comparison_plots()
    
    # Test Suite 3: Unit Conversion
    print("\n" + "="*60)
    print("  RUNNING TEST SUITE 3: UNIT CONVERSION  ")
    print("="*60 + "\n")
    test_unit_conversion()

    # Test Suite 4: Rate Validation
    print("\n" + "="*60)
    print("  RUNNING TEST SUITE 4: RATE VALIDATION & AGGREGATION  ")
    print("="*60 + "\n")
    test_rate_validation_and_aggregation()
    
    # Test Suite 5: Column Generation
    print("\n" + "="*60)
    print("  RUNNING TEST SUITE 5: COLUMN GENERATION  ")
    print("="*60 + "\n")
    test_column_generation()

    print("\n" + "="*50)
    print("  ALL BACKEND TESTS COMPLETED SUCCESSFULLY  ")
    print("="*50)