import tkinter as tk
from tkinter import ttk, messagebox
import os
from datetime import datetime
from typing import Optional
import polars as pl

import config
from backend import data_loader, data_processor, plot_generator
from .ui_components import InspectionPanel, ColumnSelectionTabs, PlotView
from .app_state import AppState


class DataFrameInspector:
    """Lightweight inspection utilities for DataFrame operations"""

    @staticmethod
    def log_step(df: pl.DataFrame, step: str, details: str = "") -> str:
        timestamp = datetime.now().strftime("%H:%M:%S")
        return (
            f"[{timestamp}] {step}: {df.shape[0]} rows, {df.shape[1]} cols {details}\n"
        )

    @staticmethod
    def inspect_dataframe(df: pl.DataFrame, step_name: str) -> str:
        lines = [
            f"\n{'=' * 70}",
            f"{step_name.upper()}",
            f"{'=' * 70}",
            f"Shape: {df.shape[0]} rows × {df.shape[1]} columns",
            f"Memory: {df.estimated_size('mb'):.2f} MB\n",
            "COLUMNS:",
        ]

        for i, col in enumerate(df.columns, 1):
            lines.append(
                f"  {i:2d}. {col:<30} ({str(df[col].dtype):<10}) - {df[col].null_count()} nulls"
            )

        lines.extend(
            [
                f"\nPREVIEW:\n{df.head(3)}\n",
                f"{'=' * 70}\n",
            ]
        )

        report = "\n".join(lines)
        print(report)
        return report


class ApplicationUI:
    """Main application interface for Excel data processing pipeline"""

    def __init__(self, root: tk.Tk):
        self.root = root
        self.state = AppState(config.EXCEL_FILE, config.OUTPUT_DIRECTORY)
        self.inspector = DataFrameInspector()

        self.stage_label: Optional[ttk.Label] = None
        self.content_frame: Optional[ttk.Frame] = None
        self.inspection_panel: Optional[InspectionPanel] = None

        self.column_tabs: Optional[ColumnSelectionTabs] = None
        self.plot_view: Optional[PlotView] = None

        self._setup_main_window()
        self._show_sheet_input()

    def _setup_main_window(self):
        """Initialize main window structure"""
        self.root.title("Excel Data Pipeline Processor")
        self.root.geometry("1200x900")
        self.root.minsize(1100, 800)
        self.root.configure(bg="#f0f0f0")
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        main_container = ttk.Frame(self.root, padding="15")
        main_container.grid(row=0, column=0, sticky="nsew")
        main_container.columnconfigure(0, weight=1)
        main_container.rowconfigure(1, weight=1)

        self._create_header(main_container)

        self.content_frame = ttk.Frame(main_container)
        self.content_frame.grid(row=1, column=0, sticky="nsew", pady=10)
        self.content_frame.columnconfigure(0, weight=1)
        self.content_frame.rowconfigure(0, weight=1)

        self.inspection_panel = InspectionPanel(main_container)
        self.inspection_panel.grid(row=2, column=0, sticky="ewns", pady=10)

    def _create_header(self, parent):
        """Create application header with title and stage indicator"""
        header = ttk.Frame(parent)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        header.columnconfigure(1, weight=1)

        ttk.Label(
            header,
            text="Excel Data Pipeline Processor",
            font=("Arial", 16, "bold"),
        ).grid(row=0, column=0, sticky="w")

        self.stage_label = ttk.Label(
            header,
            text="Stage 1: Sheet Input",
            font=("Arial", 12),
            foreground="#666666",
        )
        self.stage_label.grid(row=0, column=1, sticky="e")

        ttk.Label(
            header,
            text=f"File: {os.path.basename(self.state.excel_file)}",
            font=("Arial", 10),
            foreground="#888888",
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=2)

    def _clear_content(self):
        """Clear main content area"""
        for widget in self.content_frame.winfo_children():
            widget.destroy()

    def _update_stage(self, stage: int, title: str):
        """Update stage indicator"""
        self.stage_label.config(text=f"Stage {stage}: {title}")
        self.root.title(f"Excel Data Pipeline Processor - {title}")

    def _log_and_display(self, message: str):
        """Add to log and update display"""
        self.state.inspection_log += message
        self.inspection_panel.update_log(self.state.inspection_log)

    def _show_sheet_input(self):
        """Stage 1: Sheet name input"""
        self._clear_content()
        self._update_stage(1, "Sheet Input")
        self.inspection_panel.grid()

        frame = ttk.Frame(self.content_frame, padding="20")
        frame.grid(row=0, column=0, sticky="ew", pady=20)
        frame.columnconfigure(1, weight=1)

        ttk.Label(frame, text="Sheet Name:", font=("Arial", 11)).grid(
            row=0, column=0, sticky="w", padx=(0, 10)
        )

        sheet_entry = ttk.Entry(frame, font=("Arial", 11), width=40)
        sheet_entry.grid(row=0, column=1, sticky="ew", padx=(0, 10))
        sheet_entry.bind("<Return>", lambda e: self._load_sheet(sheet_entry.get()))
        sheet_entry.focus()

        ttk.Button(
            frame,
            text="Load Sheet",
            command=lambda: self._load_sheet(sheet_entry.get()),
        ).grid(row=0, column=2, padx=5)

        ttk.Label(
            frame,
            text="Example: 'Deswik Dump' — enter exact name and press Load",
            font=("Arial", 9),
            foreground="#666666",
        ).grid(row=1, column=0, columnspan=3, sticky="w", pady=(10, 0))

    def _load_sheet(self, sheet_name: str):
        """Load Excel sheet and proceed to column selection"""
        sheet_name = sheet_name.strip()
        if not sheet_name:
            messagebox.showwarning("Input Required", "Please enter a sheet name.")
            return

        if not os.path.exists(self.state.excel_file):
            messagebox.showerror(
                "File Not Found", f"Excel file not found: {self.state.excel_file}"
            )
            return

        try:
            print(f"Loading sheet: '{sheet_name}'...")
            self.state.df = data_loader.load_excel_with_fallback(
                self.state.excel_file, sheet_name
            )
            self.state.sheet_name = sheet_name

            self._log_and_display(
                self.inspector.log_step(
                    self.state.df, "SHEET_LOADED", f"Sheet '{sheet_name}'"
                )
            )
            self._log_and_display(
                self.inspector.inspect_dataframe(
                    self.state.df, f"Loaded Sheet: {sheet_name}"
                )
            )

            self._show_column_selection()

        except Exception as e:
            messagebox.showerror(
                "Sheet Load Error", f"Failed to load sheet '{sheet_name}': {e}"
            )

    def _show_column_selection(self):
        """Stage 2: Column categorization"""
        self._clear_content()
        self._update_stage(2, f"Column Categorization for '{self.state.sheet_name}'")
        self.inspection_panel.grid()

        container = ttk.Frame(self.content_frame)
        container.grid(row=0, column=0, sticky="nsew")
        container.columnconfigure(0, weight=1)
        container.rowconfigure(0, weight=1)

        self.column_tabs = ColumnSelectionTabs(
            container,
            self.state.df.columns,
            on_selection_change=self._update_selection_count,
        )
        self.column_tabs.grid(row=0, column=0, sticky="nsew", pady=(0, 10))

        if any(self.state.selected_columns.values()):
            self.column_tabs.set_selections(self.state.selected_columns)

        controls = ttk.Frame(container)
        controls.grid(row=1, column=0, pady=10)

        ttk.Button(
            controls, text="← Back to Sheet Input", command=self._show_sheet_input
        ).pack(side=tk.LEFT, padx=5)

        ttk.Button(
            controls, text="Preview Selection", command=self._preview_selection
        ).pack(side=tk.LEFT, padx=5)

        ttk.Button(
            controls,
            text="Proceed to Aggregation →",
            command=self._validate_and_proceed_to_aggregation,
        ).pack(side=tk.LEFT, padx=5)

        ttk.Button(
            controls, text="Clear All Selections", command=self._clear_selections
        ).pack(side=tk.LEFT, padx=5)

    def _update_selection_count(self):
        """Update window title with selection count"""
        total = self.column_tabs.get_selection_count()
        self.root.title(f"Excel Data Pipeline Processor - {total} columns selected")

    def _preview_selection(self):
        """Generate and display selection preview"""
        selected = self.column_tabs.get_selected_columns()
        all_cols = [col for cols in selected.values() for col in cols]

        if not all_cols:
            messagebox.showwarning("No Selection", "Please select at least one column.")
            return

        preview_df = self.state.df.select(all_cols)
        self._log_and_display(
            self.inspector.log_step(
                preview_df, "PREVIEW_GENERATED", f"{len(all_cols)} columns"
            )
        )
        self._log_and_display(
            self.inspector.inspect_dataframe(preview_df, "Column Selection Preview")
        )

        summary = (
            "Preview Generated Successfully!\n\n"
            f"Location: {len(selected['location'])} cols\n"
            f"Activity: {len(selected['activity'])} cols\n"
            f"Timing: {len(selected['timing'])} cols\n"
            f"Drivers: {len(selected['drivers'])} cols\n\n"
            f"Total: {len(all_cols)} columns\n"
            f"Shape: {preview_df.shape[0]} rows × {preview_df.shape[1]} columns"
        )
        messagebox.showinfo("Preview Complete", summary)

    def _clear_selections(self):
        """Clear all column selections"""
        if self.column_tabs:
            self.column_tabs.clear_all()
        self.state.selected_columns = {key: [] for key in self.state.selected_columns}
        self._update_selection_count()
        self._log_and_display(
            f"[{datetime.now().strftime('%H:%M:%S')}] SELECTIONS_CLEARED\n"
        )

    def _validate_and_proceed_to_aggregation(self):
        """Validate selections and proceed to aggregation setup"""
        selected = self.column_tabs.get_selected_columns()

        if len(selected["timing"]) != 2:
            messagebox.showerror(
                "Input Error",
                "Please select exactly two 'Timing' columns (one for start date, one for end date).",
            )
            return

        if not selected["drivers"]:
            messagebox.showerror(
                "Input Error", "Please select at least one 'Drivers' column."
            )
            return

        if not selected["location"] and not selected["activity"]:
            messagebox.showerror(
                "Input Error",
                "Please select at least one 'Location' or 'Activity' column.",
            )
            return

        self.state.selected_columns = selected
        self._show_aggregation_setup()

    def _show_aggregation_setup(self):
        """Stage 3: Aggregation parameter selection"""
        self._clear_content()
        self._update_stage(3, "Aggregation Setup")
        self.inspection_panel.grid_remove()

        frame = ttk.Frame(self.content_frame, padding="20")
        frame.grid(row=0, column=0, sticky="ew", pady=20)
        frame.columnconfigure(1, weight=1)

        ttk.Label(
            frame,
            text="Configure Time-Series Aggregation",
            font=("Arial", 14, "bold"),
        ).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 15))

        ttk.Label(frame, text="Select Start Date Column:", font=("Arial", 11)).grid(
            row=1, column=0, sticky="w", padx=(0, 10), pady=5
        )
        start_date_combo = ttk.Combobox(
            frame, values=self.state.selected_columns["timing"], state="readonly"
        )
        start_date_combo.grid(row=1, column=1, sticky="ew", pady=5)
        if self.state.selected_columns["timing"]:
            start_date_combo.set(self.state.selected_columns["timing"][0])

        ttk.Label(frame, text="Select End Date Column:", font=("Arial", 11)).grid(
            row=2, column=0, sticky="w", padx=(0, 10), pady=5
        )
        end_date_combo = ttk.Combobox(
            frame, values=self.state.selected_columns["timing"], state="readonly"
        )
        end_date_combo.grid(row=2, column=1, sticky="ew", pady=5)
        if len(self.state.selected_columns["timing"]) > 1:
            end_date_combo.set(self.state.selected_columns["timing"][1])

        ttk.Label(frame, text="Select Driver Column to Sum:", font=("Arial", 11)).grid(
            row=3, column=0, sticky="w", padx=(0, 10), pady=5
        )
        driver_combo = ttk.Combobox(
            frame, values=self.state.selected_columns["drivers"], state="readonly"
        )
        driver_combo.grid(row=3, column=1, sticky="ew", pady=5)
        if len(self.state.selected_columns["drivers"]) == 1:
            driver_combo.set(self.state.selected_columns["drivers"][0])

        controls = ttk.Frame(frame)
        controls.grid(row=4, column=0, columnspan=2, pady=(20, 0), sticky="w")

        ttk.Button(
            controls,
            text="← Back to Column Selection",
            command=self._show_column_selection,
        ).pack(side=tk.LEFT, padx=5)

        ttk.Button(
            controls,
            text="Aggregate & Export Final Data",
            command=lambda: self._process_and_aggregate(
                start_date_combo.get(), end_date_combo.get(), driver_combo.get()
            ),
        ).pack(side=tk.LEFT, padx=5)

    def _process_and_aggregate(
        self, start_date_col: str, end_date_col: str, driver_col: str
    ):
        """Execute final aggregation and export"""
        if not start_date_col or not driver_col or not end_date_col:
            messagebox.showerror(
                "Input Required",
                "Please select a start date, end date, and a driver column.",
            )
            return

        if start_date_col == end_date_col:
            messagebox.showerror(
                "Input Error", "Start date and end date columns cannot be the same."
            )
            return

        self.state.driver_col = driver_col
        grouping_cols = (
            self.state.selected_columns["location"]
            + self.state.selected_columns["activity"]
        )

        try:
            self.state.final_dataframe = data_processor.aggregate_data(
                df=self.state.df,
                grouping_cols=grouping_cols,
                start_date_col=start_date_col,
                driver_col=driver_col,
            )

            self._log_and_display(
                self.inspector.log_step(
                    self.state.final_dataframe,
                    "AGGREGATION_COMPLETE",
                    "Pivoted DataFrame created",
                )
            )
            self._log_and_display(
                self.inspector.inspect_dataframe(
                    self.state.final_dataframe, "Final Aggregated DataFrame"
                )
            )

            export_path = self.state.export_aggregated_data()

            grand_total = self.state.final_dataframe.filter(
                pl.col(grouping_cols[0]) == "GRAND TOTAL"
            )["Total"][0]

            success_msg = (
                f"🎉 DATA AGGREGATION COMPLETE! 🎉\n\n"
                f"Source Sheet: {self.state.sheet_name}\n"
                f"Final Shape: {self.state.final_dataframe.shape[0]} rows × "
                f"{self.state.final_dataframe.shape[1]} columns\n"
                f"Grand Total of '{driver_col}': {grand_total:,.2f}\n\n"
                f"Grouped by {len(grouping_cols)} columns: {', '.join(grouping_cols)}\n\n"
                f"Exported to: {export_path}\n\n"
                "Would you like to proceed to driver profile visualization?"
            )

            if messagebox.askyesno("🎉 Success!", success_msg):
                self._show_plotting()
            else:
                self._update_stage(4, "Processing Complete")

        except Exception as e:
            messagebox.showerror("Processing Error", f"Final aggregation failed: {e}")

    def _show_plotting(self):
        """Stage 4: Driver profile visualization"""
        self._clear_content()
        self._update_stage(4, "Driver Profile Visualization")
        self.inspection_panel.grid_remove()

        container = ttk.Frame(self.content_frame)
        container.grid(row=0, column=0, sticky="nsew")
        container.columnconfigure(0, weight=1)
        container.rowconfigure(1, weight=1)

        ttk.Label(
            container,
            text="Driver Profile Visualization",
            font=("Arial", 14, "bold"),
        ).grid(row=0, column=0, sticky="w", pady=(0, 15), padx=10)

        grouping_cols = (
            self.state.selected_columns["location"]
            + self.state.selected_columns["activity"]
        )

        self.plot_view = PlotView(
            container,
            self.state.final_dataframe,
            grouping_cols,
            self.state.driver_col,
            plot_generator,
        )
        self.plot_view.grid(row=1, column=0, sticky="nsew", padx=10)

        controls = ttk.Frame(container)
        controls.grid(row=2, column=0, pady=10, padx=10, sticky="w")

        ttk.Button(
            controls, text="← Back to Aggregation", command=self._show_aggregation_setup
        ).pack(side=tk.LEFT, padx=5)

        ttk.Button(
            controls, text="Export Plot as PNG", command=self.plot_view.export_plot
        ).pack(side=tk.LEFT, padx=5)


def main():
    root = tk.Tk()
    ApplicationUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
