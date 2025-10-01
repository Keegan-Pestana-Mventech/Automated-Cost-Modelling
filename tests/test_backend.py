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
    create_toy_dataframe_with_date_column
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
    aggregated_df = data_processor.aggregate_data(
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
    
    # GRAND TOTAL row is no longer created, so the check is removed.
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
    
    aggregated_df = data_processor.aggregate_data(
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
    
    aggregated_df = data_processor.aggregate_data(
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


# The `test_plotting` function has been removed as it was redundant.
# The `test_data_filtering_and_comparison_plots` function already
# generates a plot of the original aggregated data for comparison purposes.


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
    aggregated_df = data_processor.aggregate_data(
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
    # Note: The original data only goes to June. July is processed because other groups have data in July.
    # The stockpile depletion then creates the August data point.
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

    # Check that depletion stopped correctly (no '2024-09' column should be created by this group)
    # The final pivoted df might have this column if another group extends to it, but its value should be 0.
    if "2025-09" in smoothed_series.columns:
        assert smoothed_series["2025-09"][0] == 0.0, "Depletion should have stopped, 2025-09 should be 0"


    # The most important check: Conservation of mass. The total volume must be the same.
    exclude_cols = grouping_cols + ["ID"]
    original_month_cols = [c for c in original_series.columns if c not in exclude_cols]
    smoothed_month_cols = [c for c in smoothed_series.columns if c not in grouping_cols] # This line is fine as smoothed_df has no ID column

    original_total = original_series.select(pl.sum_horizontal(original_month_cols))[0,0]
    smoothed_total = smoothed_series.select(pl.sum_horizontal(smoothed_month_cols))[0,0]
    
    print(f"Original Total Volume: {original_total}")
    print(f"Smoothed Total Volume: {smoothed_total}")
    assert np.isclose(original_total, smoothed_total), "Total volume before and after smoothing must be equal!"

    print("\n[SUCCESS] Smoothing logic verified programmatically.\n")
    
    # 6. Generate comparison plots for the first data entry ("North | Gadgets")
    if aggregated_df.height > 0:
        entry_index = 0 # This corresponds to "North | Gadgets"
        label_parts = [str(aggregated_df[col][entry_index]) for col in grouping_cols]
        selected_label = " | ".join(label_parts)
        
        print(f"--- 6. Generating Comparison Plots for: '{selected_label}' ---\n")
        
        # Plot 1: Original aggregated data
        print("Generating original data plot...")
        original_figure = plot_generator.generate_plot(
            df=aggregated_df,
            entry_index=entry_index,
            selected_entry_label=f"{selected_label} (Original)",
            grouping_cols=grouping_cols,
            driver_col_name=driver_col,
            plot_settings=config.DEFAULT_PLOT_SETTINGS,
        )
        
        original_plot_path = config.OUTPUT_DIRECTORY / config.PLOT_OUTPUT_SUBDIR / "original_data_plot.png"
        original_figure.savefig(original_plot_path)
        print(f"Original plot saved to: {original_plot_path}")
        
        # Plot 2: Smoothed data
        print("Generating smoothed data plot...")
        smoothed_figure = plot_generator.generate_plot(
            df=smoothed_df,
            entry_index=entry_index,
            selected_entry_label=f"{selected_label} (Smoothed)",
            grouping_cols=grouping_cols,
            driver_col_name=driver_col,
            plot_settings=config.DEFAULT_PLOT_SETTINGS,
        )
        
        smoothed_plot_path = config.OUTPUT_DIRECTORY / config.PLOT_OUTPUT_SUBDIR / "smoothed_data_plot.png"
        smoothed_figure.savefig(smoothed_plot_path)
        print(f"Smoothed plot saved to: {smoothed_plot_path}")
        
        print("\n--- 6. Comparison Complete ---")
        print("Check both plot files to see the smoothing effect:")
        print(f"  - Original: {original_plot_path}")
        print(f"  - Smoothed: {smoothed_plot_path}")
        
    else:
        print("[SKIP] No data rows available for plotting.")

    # 7. NEW: Generate plot for the longest series to justify the extended timeline
    print(f"\n--- 7. Generating Plot for 'West | Widgets' ---")
    try:
        # Find the index of the "West | Widgets" group programmatically
        west_widgets_index = -1
        for i, row in enumerate(smoothed_df.iter_rows(named=True)):
            if row["Region"] == "West" and row["Product Line"] == "Widgets":
                west_widgets_index = i
                break

        if west_widgets_index != -1:
            selected_label = "West | Widgets"
            print(f"Found '{selected_label}' at index {west_widgets_index}. Generating plot...")

            west_figure = plot_generator.generate_plot(
                df=smoothed_df,
                entry_index=west_widgets_index,
                selected_entry_label=f"{selected_label} (Smoothed, )",
                grouping_cols=grouping_cols,
                driver_col_name=driver_col,
                plot_settings=config.DEFAULT_PLOT_SETTINGS,
            )
            west_plot_path = config.OUTPUT_DIRECTORY / config.PLOT_OUTPUT_SUBDIR / "west_widgets_plot.png"
            west_figure.savefig(west_plot_path)
            print(f" plot saved to: {west_plot_path}")
        else:
            print("Could not find 'West | Widgets' group to generate  plot.")

    except Exception as e:
        print(f"An error occurred while generating the  plot: {e}")
        
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
        "Region": ["North", "South", "East", "West", "Central", "Northeast", "Southwest"],
        "Product Line": ["Widgets", "Gadgets", "Widgets", "Gadgets", "Tools", "Equipment", "Supplies"],
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
    
    # Verify the SI Rates column was added
    assert "SI Rates" in converted_df.columns, "SI Rates column not added"
    assert converted_df.height == test_df.height, "Row count changed unexpectedly"
    
    # Test individual conversions
    print("--- 4. Verifying Individual Conversions ---")
    
    # Test case 1: 52 feet per week
    # 52 ft = 15.8496 m, per week * 4.33 = 68.63 m/mo
    result_1 = converted_df["SI Rates"][0]
    print(f"52ftp/w -> {result_1}")
    assert "meter/mo" in result_1, f"Expected meter/mo unit, got {result_1}"
    
    # Test case 2: 60 meters per month (should stay mostly the same)
    result_2 = converted_df["SI Rates"][1]
    print(f"60m/mo -> {result_2}")
    assert "meter/mo" in result_2, f"Expected meter/mo unit, got {result_2}"
    
    # Test case 3: 10 gallons per day
    # 10 gal = ~37.85 liters, per day * 30.44 = ~1152 liter/mo
    result_3 = converted_df["SI Rates"][2]
    print(f"10gal/d -> {result_3}")
    assert "liter/mo" in result_3, f"Expected liter/mo unit, got {result_3}"
    
    # Test case 4: 100 feet per year
    # 100 ft = 30.48 m, per year / 12 = 2.54 m/mo
    result_4 = converted_df["SI Rates"][3]
    print(f"100ft/yr -> {result_4}")
    assert "meter/mo" in result_4, f"Expected meter/mo unit, got {result_4}"
    
    # Test case 5: 25 wet metric tons per month
    result_5 = converted_df["SI Rates"][4]
    print(f"25wmt/mo -> {result_5}")
    assert "kilogram/mo" in result_5, f"Expected kilogram/mo unit, got {result_5}"
    
    # Test case 6: 15 dry metric tons per month
    result_6 = converted_df["SI Rates"][5]
    print(f"15dmt/mo -> {result_6}")
    assert "kilogram/mo" in result_6, f"Expected kilogram/mo unit, got {result_6}"
    
    # Test case 7: 200 decimeters per day
    # 200 dm = 20 m, per day * 30.44 = 608.8 m/mo
    result_7 = converted_df["SI Rates"][6]
    print(f"200dm/d -> {result_7}")
    assert "meter/mo" in result_7, f"Expected meter/mo unit, got {result_7}"
    
    print("\n--- 5. Testing Edge Cases ---")
    
    # Test invalid rate string
    edge_case_data = {
        "Region": ["Test1", "Test2", "Test3"],
        "Product Line": ["A", "B", "C"],
        "Rates": ["invalid_format", "50/", "abc/mo"]
    }
    edge_df = pl.DataFrame(edge_case_data)
    edge_converted = unit_converter.add_si_rate_column(edge_df, "Rates")
    
    print("Edge case conversions:")
    for i, rate in enumerate(edge_df["Rates"]):
        converted = edge_converted["SI Rates"][i]
        print(f"  {rate} -> {converted}")
        # Invalid formats should return the original string
        if rate in ["invalid_format", "50/", "abc/mo"]:
            assert converted == rate, f"Invalid format should return original string"
    
    print("\n[SUCCESS] Unit conversion test completed.\n")
    return converted_df

def test_column_generation():
    """
    Tests the column generation functionality.
    """
    print("\n" + "="*40)
    print("  TESTING COLUMN GENERATION  ")
    print("="*40 + "\n")

    # 1. Create a base DataFrame
    data = {
        'A': [1, 2, 3, 4, 5],
        'B': [10, 20, 30, 40, 50],
        'C': [2, 4, 2, 5, 10],
        'D_str': ["5", "10", "15", "20", "25"] # Test string casting
    }
    df = pl.DataFrame(data)
    print("--- 1. Original DataFrame ---")
    print(df)
    print("\n")

    # 2. Test SUM operation
    print("--- 2. Testing SUM: A + B ---")
    df_sum = column_generation.generate_new_column(df, ['A', 'B'], 'Sum_A_B', 'sum')
    print(df_sum)
    expected_sum = pl.Series("Sum_A_B", [11.0, 22.0, 33.0, 44.0, 55.0])
    assert df_sum['Sum_A_B'].equals(expected_sum)
    print("[SUCCESS] Sum operation is correct.\n")

    # 3. Test MULTIPLY operation
    print("--- 3. Testing MULTIPLY: A * C ---")
    df_mul = column_generation.generate_new_column(df, ['A', 'C'], 'Mul_A_C', 'multiply')
    print(df_mul)
    expected_mul = pl.Series("Mul_A_C", [2.0, 8.0, 6.0, 20.0, 50.0])
    assert df_mul['Mul_A_C'].equals(expected_mul)
    print("[SUCCESS] Multiply operation is correct.\n")
    
    # 4. Test DIVIDE operation
    print("--- 4. Testing DIVIDE: B / C ---")
    df_div = column_generation.generate_new_column(df, ['B', 'C'], 'Div_B_C', 'divide')
    print(df_div)
    expected_div = pl.Series("Div_B_C", [5.0, 5.0, 15.0, 8.0, 5.0])
    assert df_div['Div_B_C'].equals(expected_div)
    print("[SUCCESS] Divide operation is correct.\n")
    
    # 5. Test with string casting
    print("--- 5. Testing SUM with string cast: A + D_str ---")
    df_cast = column_generation.generate_new_column(df, ['A', 'D_str'], 'Sum_A_Dstr', 'sum')
    print(df_cast)
    expected_cast_sum = pl.Series("Sum_A_Dstr", [6.0, 12.0, 18.0, 24.0, 30.0])
    assert df_cast['Sum_A_Dstr'].equals(expected_cast_sum)
    print("[SUCCESS] Sum with string casting is correct.\n")

    # 6. Test multiple column sum
    print("--- 6. Testing MULTI-COLUMN SUM: A + B + C ---")
    df_multi_sum = column_generation.generate_new_column(df, ['A', 'B', 'C'], 'Sum_ABC', 'sum')
    print(df_multi_sum)
    expected_multi_sum = pl.Series("Sum_ABC", [13.0, 26.0, 35.0, 49.0, 65.0])
    assert df_multi_sum['Sum_ABC'].equals(expected_multi_sum)
    print("[SUCCESS] Multi-column sum is correct.\n")

    print("[SUCCESS] Column generation test completed.\n")
    return df_multi_sum

if __name__ == "__main__":
    print("Starting comprehensive backend tests...")
    
    # Test 1: Aggregation with string dates
    print("\n" + "="*60)
    print("  RUNNING TEST SUITE 1: STRING DATE AGGREGATION  ")
    print("="*60 + "\n")
    final_df_string_dates = test_aggregation_string_dates()
    # The call to test_plotting() has been removed as it's redundant.
    # The test_data_filtering_and_comparison_plots function already
    # generates an "original" plot for comparison.
    
    # Test 2: Aggregation with different date column types
    print("\n" + "="*60)
    print("  RUNNING TEST SUITE 2: DATETIME & DATE COLUMN AGGREGATION  ")
    print("="*60 + "\n")
    test_aggregation_datetime_column()
    test_aggregation_date_column()
    
    # Test 3: Data filtering with stockpile logic and comparison plots
    print("\n" + "="*60)
    print("  RUNNING TEST SUITE 3: STOCKPILE SMOOTHING & COMPARISON PLOTS  ")
    print("="*60 + "\n")
    original_df, smoothed_df = test_data_filtering_and_comparison_plots()
    
    # Test 4: Unit conversion
    print("\n" + "="*60)
    print("  RUNNING TEST SUITE 4: UNIT CONVERSION  ")
    print("="*60 + "\n")
    test_unit_conversion()
    
    # Test 5: Column Generation
    print("\n" + "="*60)
    print("  RUNNING TEST SUITE 5: COLUMN GENERATION  ")
    print("="*60 + "\n")
    test_column_generation()

    print("\n" + "="*50)
    print("  ALL BACKEND TESTS COMPLETED SUCCESSFULLY  ")
    print("="*50)