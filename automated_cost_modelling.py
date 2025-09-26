import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import polars as pl
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

class DataTypeHandler:
    """Handle data type conversion and validation for Excel data loading"""
    
    @staticmethod
    def get_safe_schema_overrides(df_sample) -> Dict[str, pl.DataType]:
        """Generate schema overrides to handle problematic columns"""
        schema_overrides = {}
        
        for column in df_sample.columns:
            try:
                col_data = df_sample[column]
                
                # Force string type for columns with conversion issues
                if col_data.dtype in [pl.Float64, pl.Float32]:
                    try:
                        min_val = col_data.min()
                        max_val = col_data.max()
                        
                        if (min_val is not None and abs(min_val) > 1e15) or \
                           (max_val is not None and abs(max_val) > 1e15):
                            schema_overrides[column] = pl.Utf8
                    except:
                        schema_overrides[column] = pl.Utf8
                        
            except Exception:
                schema_overrides[column] = pl.Utf8
        
        return schema_overrides
    
    @staticmethod
    def load_excel_with_fallback(filepath: str, sheet_name: str) -> pl.DataFrame:
        """Load Excel with progressive fallback strategies for data type issues"""
        
        # Strategy 1: Try normal loading
        try:
            df = pl.read_excel(filepath, sheet_name=sheet_name)
            print(f"✓ Sheet loaded successfully with default types")
            return df
        except Exception as e:
            print(f"⚠ Default loading failed: {str(e)}")
        
        # Strategy 2: Load with string inference disabled
        try:
            df = pl.read_excel(
                filepath, 
                sheet_name=sheet_name,
                infer_schema_length=0
            )
            print(f"✓ Sheet loaded with schema inference disabled")
            return df
        except Exception as e:
            print(f"⚠ Schema inference disabled loading failed: {str(e)}")
        
        # Strategy 3: Load small sample first, then apply schema overrides
        try:
            sample_df = pl.read_excel(
                filepath, 
                sheet_name=sheet_name,
                read_csv_options={"n_rows": 100}
            )
            schema_overrides = DataTypeHandler.get_safe_schema_overrides(sample_df)
            df = pl.read_excel(
                filepath, 
                sheet_name=sheet_name,
                schema_overrides=schema_overrides
            )
            print(f"✓ Sheet loaded with schema overrides: {len(schema_overrides)} columns converted")
            return df
            
        except Exception as e:
            print(f"⚠ Schema override loading failed: {str(e)}")
        
        # Strategy 4: Force all columns to string type
        try:
            sample_df = pl.read_excel(
                filepath, 
                sheet_name=sheet_name,
                read_csv_options={"n_rows": 10}
            )
            string_schema = {col: pl.Utf8 for col in sample_df.columns}
            df = pl.read_excel(
                filepath, 
                sheet_name=sheet_name,
                schema_overrides=string_schema
            )
            print(f"✓ Sheet loaded with all columns as strings")
            return df
            
        except Exception as e:
            print(f"✗ All loading strategies failed: {str(e)}")
            raise Exception(f"Unable to load sheet '{sheet_name}' with any strategy: {str(e)}")

class DataFrameInspector:
    """Validation and inspection utilities for DataFrame operations"""
    
    @staticmethod
    def log_step(df, step: str, details: str = "") -> str:
        """Log DataFrame transformation steps with timestamp"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] {step}: {df.shape[0]} rows, {df.shape[1]} cols {details}"
        print(log_entry)
        return f"{log_entry}\n"
    
    @staticmethod
    def inspect_dataframe(df, step_name: str = "DataFrame Inspection") -> str:
        """Comprehensive DataFrame inspection with detailed analysis"""
        inspection_report = f"\n{'='*70}\n"
        inspection_report += f"{step_name.upper()}\n"
        inspection_report += f"{'='*70}\n"
        inspection_report += f"Shape: {df.shape[0]} rows × {df.shape[1]} columns\n"
        
        try:
            memory_mb = df.estimated_size('mb')
            inspection_report += f"Memory Usage: {memory_mb:.2f} MB\n"
        except:
            inspection_report += f"Memory Usage: Unable to calculate\n"
        
        inspection_report += f"\nCOLUMN ANALYSIS:\n"
        for i, col in enumerate(df.columns, 1):
            try:
                null_count = df[col].null_count()
            except Exception:
                null_count = "?"
            data_type = str(df[col].dtype)
            inspection_report += f"  {i:2d}. {col:<30} ({data_type:<10}) - {null_count} nulls\n"
        
        inspection_report += f"\nDATA PREVIEW (First 3 rows):\n{df.head(3)}\n"
        
        try:
            inspection_report += f"\nNULL VALUE SUMMARY:\n{df.null_count()}\n"
        except:
            inspection_report += f"\nNULL VALUE SUMMARY: Unable to compute\n"
        
        type_counts = {}
        for col in df.columns:
            dtype_str = str(df[col].dtype)
            type_counts[dtype_str] = type_counts.get(dtype_str, 0) + 1
        
        inspection_report += f"\nDATA TYPE DISTRIBUTION:\n"
        for dtype, count in sorted(type_counts.items()):
            inspection_report += f"  {dtype}: {count} columns\n"
        
        inspection_report += f"{'='*70}\n"
        
        print(inspection_report)
        return inspection_report
    
    @staticmethod
    def export_checkpoint(df, filename_prefix: str, directory: str = ".") -> Optional[str]:
        """Export DataFrame checkpoint for external validation (kept but we won't call except on final)"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{filename_prefix}_{timestamp}.csv"
        filepath = Path(directory) / filename
        
        try:
            df.write_csv(filepath)
            success_msg = f"✓ Checkpoint exported: {filepath}"
            print(success_msg)
            return str(filepath)
        except Exception as e:
            error_msg = f"✗ Export failed: {e}"
            print(error_msg)
            return None

class ExcelProcessor:
    """Main application class for Excel sheet processing with column categorization"""
    
    def __init__(self):
        self.excel_file = "REDISTRIBUTED MY26 Deswik Dump Summary Mine Physicals 22 August 2025.xlsx"
        self.output_directory = "data"
        Path(self.output_directory).mkdir(exist_ok=True)
        self.sheet_name = None
        self.df = None
        self.final_dataframe = None
        
        self.selected_columns = {
            'location': [],
            'activity': [],
            'timing': [],
            'drivers': []
        }
        
        self.inspector = DataFrameInspector()
        self.data_handler = DataTypeHandler()
        self.inspection_log = ""
        
        self.root = None
        self.current_stage = "sheet_input"
        self.checkboxes = {}
        self.tab_widgets = {}

    def validate_file_exists(self) -> bool:
        """Validate that the Excel file exists"""
        if not os.path.exists(self.excel_file):
            error_msg = f"Excel file not found: {self.excel_file}"
            print(f"✗ {error_msg}")
            messagebox.showerror("File Not Found", error_msg)
            return False
        return True
    
    def load_sheet_data(self, sheet_name: str) -> bool:
        """Load data from specified sheet with data type handling"""
        try:
            print(f"Loading sheet: '{sheet_name}'...")
            self.df = self.data_handler.load_excel_with_fallback(self.excel_file, sheet_name)
            self.sheet_name = sheet_name
            
            log_entry = self.inspector.log_step(
                self.df, "SHEET_LOADED", f"Sheet '{sheet_name}' loaded"
            )
            self.inspection_log += log_entry
            
            inspection_report = self.inspector.inspect_dataframe(
                self.df, f"Loaded Sheet: {sheet_name}"
            )
            self.inspection_log += inspection_report
            
            return True
            
        except Exception as e:
            error_msg = f"Failed to load sheet '{sheet_name}': {str(e)}"
            print(f"✗ {error_msg}")
            messagebox.showerror("Sheet Load Error", error_msg)
            return False
    
    def create_main_gui(self):
        """Create main GUI container"""
        self.root = tk.Tk()
        self.root.title("Excel Data Pipeline Processor")
        self.root.geometry("1200x900")
        self.root.configure(bg='#f0f0f0')
        
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        
        self.main_container = ttk.Frame(self.root, padding="15")
        self.main_container.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.main_container.columnconfigure(0, weight=1)
        self.main_container.rowconfigure(1, weight=1)
        
        self.create_application_header()
        
        self.content_frame = ttk.Frame(self.main_container)
        self.content_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=10)
        self.content_frame.columnconfigure(0, weight=1)
        self.content_frame.rowconfigure(0, weight=1)
        
        self.create_sheet_input_interface()
        
        self.create_inspection_panel()
    
    def create_application_header(self):
        """Create application header with stage indicator"""
        header_frame = ttk.Frame(self.main_container)
        header_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        header_frame.columnconfigure(1, weight=1)
        
        title_label = ttk.Label(header_frame, text="Excel Data Pipeline Processor", font=('Arial', 16, 'bold'))
        title_label.grid(row=0, column=0, sticky=tk.W)
        
        self.stage_label = ttk.Label(header_frame, text="Stage 1: Sheet Input", font=('Arial', 12), foreground='#666666')
        self.stage_label.grid(row=0, column=1, sticky=tk.E)
        
        file_info = ttk.Label(header_frame, text=f"File: {os.path.basename(self.excel_file)}", font=('Arial', 10), foreground='#888888')
        file_info.grid(row=1, column=0, columnspan=2, sticky=tk.W, pady=2)
    
    def create_sheet_input_interface(self):
        """Stage 1: Simplified sheet name input interface (minimal)"""
        for widget in self.content_frame.winfo_children():
            widget.destroy()
        
        input_frame = ttk.Frame(self.content_frame, padding="20")
        input_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=20)
        input_frame.columnconfigure(1, weight=1)
        
        sheet_label = ttk.Label(input_frame, text="Sheet Name:", font=('Arial', 11))
        sheet_label.grid(row=0, column=0, sticky=tk.W, padx=(0, 10))
        
        self.sheet_entry = ttk.Entry(input_frame, font=('Arial', 11), width=40)
        self.sheet_entry.grid(row=0, column=1, sticky=(tk.W, tk.E), padx=(0, 10))
        self.sheet_entry.bind('<Return>', lambda e: self.load_sheet())
        
        load_button = ttk.Button(input_frame, text="Load Sheet", command=self.load_sheet)
        load_button.grid(row=0, column=2, padx=5)
        
        example_label = ttk.Label(input_frame, text="Example: 'Deswik Dump' — enter exact sheet name and press Load", font=('Arial', 9), foreground='#666666')
        example_label.grid(row=1, column=0, columnspan=3, sticky=tk.W, pady=(10, 0))
        
        self.sheet_entry.focus()
    
    def load_sheet(self):
        """Load the specified sheet (then go straight to column selection)"""
        sheet_name = self.sheet_entry.get().strip()
        if not sheet_name:
            messagebox.showwarning("Input Required", "Please enter a sheet name.")
            return
        if not self.validate_file_exists():
            return
        if self.load_sheet_data(sheet_name):
            self.update_inspection_log_display()
            self.proceed_to_column_selection()
    
    def proceed_to_column_selection(self):
        """Transition to column selection stage"""
        self.current_stage = "column_selection"
        self.stage_label.config(text="Stage 2: Column Categorization")
        self.root.title(f"Excel Data Pipeline Processor - Column Selection ({self.sheet_name})")
        self.create_column_selection_interface()
    
    def create_column_selection_interface(self):
        """Stage 2: Create column categorization interface"""
        for widget in self.content_frame.winfo_children():
            widget.destroy()
        
        selection_container = ttk.Frame(self.content_frame)
        selection_container.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        selection_container.columnconfigure(0, weight=1)
        selection_container.rowconfigure(0, weight=1)
        
        notebook = ttk.Notebook(selection_container)
        notebook.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        
        sections = [
            ("Location Descriptors", "location", "Select columns that describe locations, areas, or spatial identifiers"),
            ("Activity/Work Type Descriptors", "activity", "Select columns that describe the type of work, task, or activity"),
            ("Timing", "timing", "Select columns related to time periods, dates, or scheduling"),
            ("Drivers", "drivers", "Select columns containing cost drivers, quantities, or operational metrics")
        ]
        
        self.checkboxes = {}
        self.tab_widgets = {}
        
        for title, key, description in sections:
            tab_frame = ttk.Frame(notebook, padding="15")
            notebook.add(tab_frame, text=title)
            tab_frame.columnconfigure(0, weight=1)
            tab_frame.rowconfigure(2, weight=1)
            
            desc_label = ttk.Label(tab_frame, text=description, font=('Arial', 10), wraplength=800)
            desc_label.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 10))

            search_frame = ttk.Frame(tab_frame)
            search_frame.grid(row=1, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
            ttk.Label(search_frame, text="🔍 Filter:").pack(side=tk.LEFT, padx=(0, 5))
            search_entry = ttk.Entry(search_frame)
            search_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
            
            canvas = tk.Canvas(tab_frame)
            scrollbar = ttk.Scrollbar(tab_frame, orient="vertical", command=canvas.yview)
            scrollable_frame = ttk.Frame(canvas)
            
            scrollable_frame.bind("<Configure>", lambda e, c=canvas: c.configure(scrollregion=c.bbox("all")))
            
            canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
            canvas.configure(yscrollcommand=scrollbar.set)
            
            canvas.grid(row=2, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
            scrollbar.grid(row=2, column=1, sticky=(tk.N, tk.S))
            
            search_entry.bind('<KeyRelease>', lambda e, k=key, c=canvas: self.filter_columns(k, c))
            self.tab_widgets[key] = {'search_entry': search_entry, 'canvas': canvas}

            self.checkboxes[key] = {}
            for i, column in enumerate(self.df.columns):
                var = tk.BooleanVar()
                checkbox = ttk.Checkbutton(scrollable_frame, text=column, variable=var, command=self.update_selection_count)
                checkbox.grid(row=i//2, column=i%2, sticky=tk.W, padx=10, pady=3)
                self.checkboxes[key][column] = {'var': var, 'widget': checkbox, 'row': i // 2, 'col': i % 2}
        
        control_frame = ttk.Frame(selection_container)
        control_frame.grid(row=1, column=0, pady=10)
        
        back_button = ttk.Button(control_frame, text="← Back to Sheet Input", command=self.back_to_sheet_input)
        back_button.pack(side=tk.LEFT, padx=5)
        
        preview_button = ttk.Button(control_frame, text="Preview Selection", command=self.preview_column_selection)
        preview_button.pack(side=tk.LEFT, padx=5)
        
        process_button = ttk.Button(control_frame, text="Proceed to Aggregation →", command=self.proceed_to_aggregation_setup)
        process_button.pack(side=tk.LEFT, padx=5)
        
        clear_button = ttk.Button(control_frame, text="Clear All Selections", command=self.clear_all_selections)
        clear_button.pack(side=tk.LEFT, padx=5)
    
    def filter_columns(self, category_key: str, canvas: tk.Canvas):
        """Filter checkboxes in a given category based on search term."""
        search_term = self.tab_widgets[category_key]['search_entry'].get().lower()
        
        for column, data in self.checkboxes[category_key].items():
            widget = data['widget']
            if search_term in column.lower():
                widget.grid(row=data['row'], column=data['col'], sticky=tk.W, padx=10, pady=3)
            else:
                widget.grid_remove()
        
        canvas.update_idletasks()
        canvas.configure(scrollregion=canvas.bbox("all"))

    def update_selection_count(self):
        """Update selection count in window title"""
        total_selected = sum(sum(data['var'].get() for data in section.values()) for section in self.checkboxes.values())
        self.root.title(f"Excel Data Pipeline Processor - Column Selection ({self.sheet_name}) - {total_selected} selected")
    
    def get_selected_columns(self) -> List[str]:
        """Get all selected columns across categories"""
        selected = []
        for key in self.selected_columns: self.selected_columns[key] = []
        
        for category, checkboxes in self.checkboxes.items():
            for column, data in checkboxes.items():
                if data['var'].get():
                    selected.append(column)
                    self.selected_columns[category].append(column)
        return selected
    
    def preview_column_selection(self):
        """Preview selected columns and data (no exports here)"""
        selected_cols = self.get_selected_columns()
        if not selected_cols:
            messagebox.showwarning("No Selection", "Please select at least one column.")
            return
        
        try:
            preview_df = self.df.select(selected_cols)
            log_entry = self.inspector.log_step(preview_df, "PREVIEW_GENERATED", f"Selected {len(selected_cols)} columns")
            self.inspection_log += log_entry
            
            inspection_report = self.inspector.inspect_dataframe(preview_df, "Column Selection Preview")
            self.inspection_log += inspection_report
            
            self.update_inspection_log_display()
            
            summary = f"Preview Generated Successfully!\n\n"
            summary += f"Location Descriptors: {len(self.selected_columns['location'])} columns\n"
            summary += f"Activity/Work Type Descriptors: {len(self.selected_columns['activity'])} columns\n"
            summary += f"Timing: {len(self.selected_columns['timing'])} columns\n"
            summary += f"Drivers: {len(self.selected_columns['drivers'])} columns\n"
            summary += f"\nTotal Selected: {len(selected_cols)} columns\n"
            summary += f"Final Data Shape: {preview_df.shape[0]} rows × {preview_df.shape[1]} columns"
            messagebox.showinfo("Preview Complete", summary)
            
        except Exception as e:
            messagebox.showerror("Preview Error", f"Preview generation failed: {str(e)}")

    def proceed_to_aggregation_setup(self):
        """Validate selection and move to the aggregation setup screen."""
        self.get_selected_columns()
        if not self.selected_columns['timing']:
            messagebox.showerror("Input Error", "Please select at least one 'Timing' column.")
            return
        if not self.selected_columns['drivers']:
            messagebox.showerror("Input Error", "Please select at least one 'Drivers' column.")
            return
        if not self.selected_columns['location'] and not self.selected_columns['activity']:
             messagebox.showerror("Input Error", "Please select at least one 'Location' or 'Activity' column to group by.")
             return

        self.current_stage = "aggregation_setup"
        self.stage_label.config(text="Stage 3: Aggregation Setup")
        self.root.title(f"Excel Data Pipeline Processor - Aggregation Setup ({self.sheet_name})")
        self.create_aggregation_setup_interface()

    def create_aggregation_setup_interface(self):
        """Stage 3: Create interface for selecting aggregation columns."""
        for widget in self.content_frame.winfo_children():
            widget.destroy()

        setup_frame = ttk.Frame(self.content_frame, padding="20")
        setup_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=20)
        setup_frame.columnconfigure(1, weight=1)

        title_label = ttk.Label(setup_frame, text="Configure Time-Series Aggregation", font=('Arial', 14, 'bold'))
        title_label.grid(row=0, column=0, columnspan=2, sticky=tk.W, pady=(0, 15))

        # Start Date Selection
        ttk.Label(setup_frame, text="Select Start Date Column:", font=('Arial', 11)).grid(row=1, column=0, sticky=tk.W, padx=(0, 10), pady=5)
        self.start_date_combo = ttk.Combobox(setup_frame, values=self.selected_columns['timing'], state="readonly")
        self.start_date_combo.grid(row=1, column=1, sticky=(tk.W, tk.E), pady=5)

        # End Date Selection
        ttk.Label(setup_frame, text="Select End Date Column:", font=('Arial', 11)).grid(row=2, column=0, sticky=tk.W, padx=(0, 10), pady=5)
        self.end_date_combo = ttk.Combobox(setup_frame, values=self.selected_columns['timing'], state="readonly")
        self.end_date_combo.grid(row=2, column=1, sticky=(tk.W, tk.E), pady=5)

        # Driver Selection
        ttk.Label(setup_frame, text="Select Driver Column to Sum:", font=('Arial', 11)).grid(row=3, column=0, sticky=tk.W, padx=(0, 10), pady=5)
        self.driver_combo = ttk.Combobox(setup_frame, values=self.selected_columns['drivers'], state="readonly")
        self.driver_combo.grid(row=3, column=1, sticky=(tk.W, tk.E), pady=5)
        
        # Pre-select if only one option is available
        if len(self.selected_columns['timing']) > 0: self.start_date_combo.set(self.selected_columns['timing'][0])
        if len(self.selected_columns['timing']) > 1: self.end_date_combo.set(self.selected_columns['timing'][1])
        if len(self.selected_columns['drivers']) == 1: self.driver_combo.set(self.selected_columns['drivers'][0])
        
        control_frame = ttk.Frame(setup_frame)
        control_frame.grid(row=4, column=0, columnspan=2, pady=(20, 0), sticky=tk.W)

        back_button = ttk.Button(control_frame, text="← Back to Column Selection", command=self.proceed_to_column_selection)
        back_button.pack(side=tk.LEFT, padx=5)

        process_button = ttk.Button(control_frame, text="Aggregate & Export Final Data", command=self.process_and_aggregate_data)
        process_button.pack(side=tk.LEFT, padx=5)

    def process_and_aggregate_data(self):
        """Process final selection, create aggregated DataFrame, and export."""
        start_date_col = self.start_date_combo.get()
        driver_col = self.driver_combo.get()

        if not start_date_col or not driver_col:
            messagebox.showerror("Input Required", "Please select a start date and a driver column.")
            return

        grouping_cols = self.selected_columns['location'] + self.selected_columns['activity']
        if not grouping_cols:
            messagebox.showerror("Processing Error", "No columns selected for grouping (Location or Activity).")
            return

        try:
            print(f"Starting aggregation. Grouping by: {grouping_cols}")
            
            # 1. Prepare DataFrame for aggregation
            df_agg = self.df.select(grouping_cols + [start_date_col, driver_col])
            
            # 2. Clean and transform columns
            df_transformed = df_agg.with_columns(
                pl.col(start_date_col).str.to_datetime(strict=False).alias("parsed_date"),
                pl.col(driver_col).cast(pl.Float64, strict=False).fill_null(0).alias("numeric_driver")
            ).filter(
                pl.col("parsed_date").is_not_null()
            ).with_columns(
                pl.col("parsed_date").dt.truncate("1mo").alias("month_period")
            )

            # 3. Perform the pivot operation
            self.final_dataframe = df_transformed.pivot(
                values="numeric_driver",
                index=grouping_cols,
                on="month_period",
                aggregate_function="sum"
            ).sort(grouping_cols)
            
            # 4. Sort the pivoted month columns chronologically (NEW)
            # Isolate the new month columns created by the pivot
            month_cols = sorted([
                col for col in self.final_dataframe.columns if col not in grouping_cols
            ])
            
            # Re-order the DataFrame to ensure dates are sequential
            self.final_dataframe = self.final_dataframe.select(
                grouping_cols + month_cols
            )

            # 5. Add the total column 
            if month_cols:
                self.final_dataframe = self.final_dataframe.with_columns(
                    Total=pl.sum_horizontal(month_cols)
                )
            grand_totals = {}
            for col in self.final_dataframe.columns:
                if col in grouping_cols:
                    grand_totals[col] = "GRAND TOTAL"
                else:
                    grand_totals[col] = self.final_dataframe[col].sum()
            total_row = pl.DataFrame([grand_totals])
            self.final_dataframe = pl.concat([self.final_dataframe, total_row], how="vertical")

            grand_total = self.final_dataframe["Total"].sum()

            # 6. Log and Export
            log_entry = self.inspector.log_step(self.final_dataframe, "AGGREGATION_COMPLETE", "Pivoted DataFrame created")
            self.inspection_log += log_entry
            inspection_report = self.inspector.inspect_dataframe(self.final_dataframe, "Final Aggregated DataFrame")
            self.inspection_log += inspection_report
            
            output_dir = Path(self.output_directory)
            for p in output_dir.iterdir():
                if p.is_file(): 
                    p.unlink()
            
            safe_name = self.sheet_name.replace(" ", "_")
            final_export_path = output_dir / f"{safe_name}_aggregated.csv"
            self.final_dataframe.write_csv(final_export_path)
            
            self.update_inspection_log_display()
            self.stage_label.config(text="Stage 4: Processing Complete")
            self.root.title("Excel Data Pipeline Processor - Processing Complete")
            
            success_msg = f"🎉 DATA AGGREGATION COMPLETE! 🎉\n\n"
            success_msg += f"Source Sheet: {self.sheet_name}\n"
            success_msg += f"Final Aggregated Shape: {self.final_dataframe.shape[0]} rows × {self.final_dataframe.shape[1]} columns\n"
            success_msg += f"Grand Total of '{driver_col}': {grand_total:,.2f}\n\n"
            success_msg += f"Grouped by {len(grouping_cols)} columns:\n• {', '.join(grouping_cols)}\n\n"
            success_msg += f"Exported to: {final_export_path}"
            messagebox.showinfo("🎉 Success!", success_msg)

        except Exception as e:
            messagebox.showerror("Processing Error", f"Final aggregation failed: {str(e)}")
            print(f"✗ Aggregation failed: {str(e)}")

    def back_to_sheet_input(self):
        """Navigate back to sheet input"""
        self.current_stage = "sheet_input"
        self.stage_label.config(text="Stage 1: Sheet Input")
        self.root.title("Excel Data Pipeline Processor")
        self.create_sheet_input_interface()
    
    def clear_all_selections(self):
        """Clear all column selections"""
        for section in self.checkboxes.values():
            for data in section.values():
                data['var'].set(False)
        self.update_selection_count()
        log_entry = f"[{datetime.now().strftime('%H:%M:%S')}] SELECTIONS_CLEARED: All column selections reset\n"
        self.inspection_log += log_entry
        self.update_inspection_log_display()
    
    def create_inspection_panel(self):
        """Create inspection and validation log panel"""
        inspect_frame = ttk.LabelFrame(self.main_container, text="Validation & Inspection Log", padding="10")
        inspect_frame.grid(row=2, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), pady=10)
        inspect_frame.columnconfigure(0, weight=1)
        inspect_frame.rowconfigure(0, weight=1)
        
        self.inspection_log_text = scrolledtext.ScrolledText(inspect_frame, height=12, width=100, font=('Consolas', 9), wrap=tk.WORD)
        self.inspection_log_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        log_control_frame = ttk.Frame(inspect_frame)
        log_control_frame.grid(row=1, column=0, sticky=tk.E, pady=5)
        
        clear_log_button = ttk.Button(log_control_frame, text="Clear Log", command=self.clear_inspection_log)
        clear_log_button.pack(side=tk.RIGHT, padx=5)
        
        export_log_button = ttk.Button(log_control_frame, text="Export Log", command=self.export_inspection_log)
        export_log_button.pack(side=tk.RIGHT, padx=5)
    
    def update_inspection_log_display(self):
        """Update inspection log display"""
        self.inspection_log_text.delete(1.0, tk.END)
        self.inspection_log_text.insert(tk.END, self.inspection_log)
        self.inspection_log_text.see(tk.END)
    
    def clear_inspection_log(self):
        """Clear inspection log"""
        self.inspection_log = ""
        self.update_inspection_log_display()
    
    def export_inspection_log(self):
        """Export inspection log to file"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"inspection_log_{timestamp}.txt"
        try:
            with open(log_filename, 'w') as f:
                f.write("EXCEL DATA PIPELINE PROCESSOR - INSPECTION LOG\n" + "="*70 + "\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Source File: {self.excel_file}\n")
                if self.sheet_name: f.write(f"Selected Sheet: {self.sheet_name}\n")
                f.write("="*70 + "\n\n" + self.inspection_log)
            messagebox.showinfo("Log Exported", f"Inspection log exported to: {log_filename}")
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export log: {str(e)}")
    
    def run(self) -> Optional[pl.DataFrame]:
        """Main execution method"""
        print("="*70 + "\nEXCEL DATA PIPELINE PROCESSOR\n" + "="*70)
        self.create_main_gui()
        self.root.mainloop()
        
        if self.final_dataframe is not None:
            print("\n" + "="*70 + "\nFINAL PROCESSING SUMMARY\n" + "="*70)
            self.inspector.inspect_dataframe(self.final_dataframe, "Pipeline Output")
            print(f"✓ SUCCESS: DataFrame ready for next pipeline stage")
            return self.final_dataframe
        else:
            print("\n⚠ Processing incomplete - No DataFrame returned")
            return None

def main() -> Optional[pl.DataFrame]:
    """Main entry point with comprehensive error handling"""
    try:
        processor = ExcelProcessor()
        return processor.run()
    except Exception as e:
        print(f"\n❌ CRITICAL PIPELINE ERROR: {str(e)}")
        return None

if __name__ == "__main__":
    print("Starting Excel Data Pipeline Processor...")
    final_dataframe = main()
    
    if final_dataframe is not None:
        print("\n" + "="*70 + "\nPIPELINE EXECUTION COMPLETE\n" + "="*70)
        print("✓ DataFrame available as 'final_dataframe' variable")
    else:
        print("\n" + "="*70 + "\nPIPELINE EXECUTION TERMINATED\n" + "="*70)
        print("⚠ No output generated")