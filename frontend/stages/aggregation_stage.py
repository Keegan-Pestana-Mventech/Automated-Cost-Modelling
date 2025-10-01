import tkinter as tk
from tkinter import ttk, messagebox
import polars as pl

import config
from backend import data_processor


class AggregationStage:
    """Stage 3: Aggregation parameter selection and execution"""

    def __init__(self, context, on_back, on_success):
        self.context = context
        self.on_back = on_back
        self.on_success = on_success

    def show(self):
        """Display the aggregation setup interface"""
        self.context.clear_content()
        self.context.update_stage(3, "Aggregation Setup")

        frame = ttk.Frame(self.context.content_frame, padding="20")
        frame.grid(row=0, column=0, sticky="ew", pady=20)
        frame.columnconfigure(1, weight=1)

        self._create_header(frame)
        start_date_combo = self._create_start_date_selector(frame)
        end_date_combo = self._create_end_date_selector(frame)
        driver_combo = self._create_driver_selector(frame)
        self._create_controls(frame, start_date_combo, end_date_combo, driver_combo)

    def _create_header(self, parent):
        """Create stage header"""
        ttk.Label(
            parent,
            text="Configure Time-Series Aggregation",
            font=config.FONT_CONFIG["plot_header"],
        ).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 15))

    def _create_start_date_selector(self, parent):
        """Create start date column selector"""
        ttk.Label(
            parent, text="Select Start Date Column:", font=config.FONT_CONFIG["label"]
        ).grid(row=1, column=0, sticky="w", padx=(0, 10), pady=5)

        combo = ttk.Combobox(
            parent,
            values=self.context.state.selected_columns["timing"],
            state="readonly",
        )
        combo.grid(row=1, column=1, sticky="ew", pady=5)

        if self.context.state.selected_columns["timing"]:
            combo.set(self.context.state.selected_columns["timing"][0])

        return combo

    def _create_end_date_selector(self, parent):
        """Create end date column selector"""
        ttk.Label(
            parent, text="Select End Date Column:", font=config.FONT_CONFIG["label"]
        ).grid(row=2, column=0, sticky="w", padx=(0, 10), pady=5)

        combo = ttk.Combobox(
            parent,
            values=self.context.state.selected_columns["timing"],
            state="readonly",
        )
        combo.grid(row=2, column=1, sticky="ew", pady=5)

        if len(self.context.state.selected_columns["timing"]) > 1:
            combo.set(self.context.state.selected_columns["timing"][1])

        return combo

    def _create_driver_selector(self, parent):
        """Create driver column selector"""
        ttk.Label(
            parent,
            text="Select Driver Column to Sum:",
            font=config.FONT_CONFIG["label"],
        ).grid(row=3, column=0, sticky="w", padx=(0, 10), pady=5)

        combo = ttk.Combobox(
            parent,
            values=self.context.state.selected_columns["drivers"],
            state="readonly",
        )
        combo.grid(row=3, column=1, sticky="ew", pady=5)

        if len(self.context.state.selected_columns["drivers"]) == 1:
            combo.set(self.context.state.selected_columns["drivers"][0])

        return combo

    def _create_controls(self, parent, start_date_combo, end_date_combo, driver_combo):
        """Create control buttons"""
        controls = ttk.Frame(parent)
        controls.grid(row=4, column=0, columnspan=2, pady=(20, 0), sticky="w")

        ttk.Button(
            controls,
            text="← Back to Column Selection",
            command=self.on_back,
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

        self.context.state.driver_col = driver_col
        grouping_cols = (
            self.context.state.selected_columns["location"]
            + self.context.state.selected_columns["activity"]
        )

        try:
            self.context.state.final_dataframe = data_processor.aggregate_data(
                df=self.context.state.df,
                grouping_cols=grouping_cols,
                start_date_col=start_date_col,
                driver_col=driver_col,
            )

            self.context.log(
                self.context.inspector.log_step(
                    self.context.state.final_dataframe,
                    "AGGREGATION_COMPLETE",
                    "Pivoted DataFrame created",
                )
            )
            self.context.log(
                self.context.inspector.inspect_dataframe(
                    self.context.state.final_dataframe, "Final Aggregated DataFrame"
                )
            )

            final_df = self.context.state.final_dataframe

            # Calculate the grand total correctly
            total = 0
            if final_df.height > 0:
                # Identify month columns (all columns except grouping columns and ID)
                month_cols = [
                    col for col in final_df.columns 
                    if col not in grouping_cols and col != "ID"
                ]
                
                if month_cols:
                    # Sum each row across month columns, then sum the resulting column of row totals
                    total_sum = (
                        final_df
                        .select(pl.sum_horizontal(pl.col(month_cols)))
                        .sum()
                        .item()
                    )
                    total = total_sum if total_sum is not None else 0

            export_path = self.context.state.export_aggregated_data()

            success_msg = (
                f"DATA AGGREGATION COMPLETE!\n\n"
                f"Source Sheet: {self.context.state.sheet_name}\n"
                f"Final Shape: {final_df.shape[0]} rows × "
                f"{final_df.shape[1]} columns\n"
                f"Total of '{driver_col}': {total:,.2f}\n\n"
                f"Grouped by {len(grouping_cols)} columns: {', '.join(grouping_cols)}\n\n"
                f"Exported to: {export_path}\n\n"
                "Would you like to proceed to driver profile visualization?"
            )

            if messagebox.askyesno("🎉 Success!", success_msg):
                self.on_success()
            else:
                self.context.update_stage(4, "Processing Complete")

        except Exception as e:
            messagebox.showerror("Processing Error", f"Final aggregation failed: {e}")