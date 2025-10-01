import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime

import config
from ..ui_components import ColumnSelectionTabs


class ColumnSelectionStage:
    """Stage 2: Column categorization"""

    def __init__(self, context, on_back, on_proceed):
        self.context = context
        self.on_back = on_back
        self.on_proceed = on_proceed
        self.column_tabs = None

    def show(self):
        """Display the column selection interface"""
        self.context.clear_content()
        self.context.update_stage(
            2, f"Column Categorization for '{self.context.state.sheet_name}'"
        )

        container = ttk.Frame(self.context.content_frame)
        container.grid(row=0, column=0, sticky="nsew")
        container.columnconfigure(0, weight=1)
        container.rowconfigure(0, weight=1)

        self._create_column_tabs(container)
        self._create_controls(container)
        self._load_initial_selections()

    def _create_column_tabs(self, parent):
        """Create the column selection tabs"""
        self.column_tabs = ColumnSelectionTabs(
            parent,
            self.context.state.df.columns,
            on_selection_change=self._update_selection_count,
        )
        self.column_tabs.grid(row=0, column=0, sticky="nsew", pady=(0, 10))

    def _create_controls(self, parent):
        """Create control buttons"""
        controls = ttk.Frame(parent)
        controls.grid(row=1, column=0, pady=10)

        ttk.Button(controls, text="← Back to Sheet Input", command=self.on_back).pack(
            side=tk.LEFT, padx=5
        )

        ttk.Button(
            controls,
            text="Load Default Selections",
            command=self._load_default_selections,
        ).pack(side=tk.LEFT, padx=5)

        ttk.Button(
            controls, text="Preview Selection", command=self._preview_selection
        ).pack(side=tk.LEFT, padx=5)

        ttk.Button(
            controls,
            text="Proceed to Transformation →",
            command=self._validate_and_proceed,
        ).pack(side=tk.LEFT, padx=5)

        ttk.Button(
            controls, text="Clear All Selections", command=self._clear_selections
        ).pack(side=tk.LEFT, padx=5)

    def _load_initial_selections(self):
        """Load default or previous selections"""
        if not any(self.context.state.selected_columns.values()):
            self._load_default_selections()
        else:
            # Restore previous selections if going back
            self.column_tabs.set_selections(self.context.state.selected_columns)

    def _load_default_selections(self):
        """Load default column selections from config"""
        available_columns = set(self.context.state.df.columns)
        filtered_selections = {}

        for category, columns in config.DEFAULT_COLUMN_SELECTIONS.items():
            filtered_selections[category] = [
                col for col in columns if col in available_columns
            ]

        self.column_tabs.set_selections(filtered_selections)
        self.context.state.selected_columns = filtered_selections

        loaded_count = sum(len(cols) for cols in filtered_selections.values())
        missing_count = (
            sum(
                len(config.DEFAULT_COLUMN_SELECTIONS[cat])
                for cat in config.DEFAULT_COLUMN_SELECTIONS
            )
            - loaded_count
        )

        log_msg = f"[{datetime.now().strftime('%H:%M:%S')}] DEFAULT_SELECTIONS_LOADED: {loaded_count} columns"
        if missing_count > 0:
            log_msg += f" ({missing_count} default columns not found in sheet)"
        log_msg += "\n"

        self.context.log(log_msg)

        if missing_count > 0:
            messagebox.showinfo(
                "Default Selections Loaded",
                f"Loaded {loaded_count} default column selections.\n\n"
                f"Note: {missing_count} default column(s) were not found in this sheet.",
            )
        else:
            messagebox.showinfo(
                "Default Selections Loaded",
                f"Successfully loaded {loaded_count} default column selections.",
            )

    def _update_selection_count(self):
        """Update window title with selection count"""
        total = self.column_tabs.get_selection_count()
        self.context.root.title(
            f"Excel Data Pipeline Processor - {total} columns selected"
        )

    def _preview_selection(self):
        """Generate and display selection preview"""
        selected = self.column_tabs.get_selected_columns()
        all_cols = [col for cols in selected.values() for col in cols]

        if not all_cols:
            messagebox.showwarning("No Selection", "Please select at least one column.")
            return

        preview_df = self.context.state.df.select(all_cols)
        self.context.log(
            self.context.inspector.log_step(
                preview_df, "PREVIEW_GENERATED", f"{len(all_cols)} columns"
            )
        )
        self.context.log(
            self.context.inspector.inspect_dataframe(
                preview_df, "Column Selection Preview"
            )
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
        self.context.state.selected_columns = {
            key: [] for key in self.context.state.selected_columns
        }
        self._update_selection_count()
        self.context.log(
            f"[{datetime.now().strftime('%H:%M:%S')}] SELECTIONS_CLEARED\n"
        )

    def _validate_and_proceed(self):
        """Validate selections and proceed to transformation setup"""
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

        self.context.state.selected_columns = selected
        self.on_proceed()