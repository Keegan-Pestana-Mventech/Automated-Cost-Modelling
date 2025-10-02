import tkinter as tk
from tkinter import ttk, Listbox, messagebox, Toplevel
from typing import Dict, List, Callable, Optional
from datetime import datetime

import polars as pl
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import config
from backend.data_filter import smooth_data_with_stockpile
import re


class PlotView(ttk.Frame):
    """Visualization panel for driver profile plots with interactive filtering and customization"""

    def __init__(
        self,
        parent,
        dataframe: pl.DataFrame,
        grouping_cols: List[str],
        driver_col: str,
    ):
        super().__init__(parent)
        self.dataframe = dataframe
        self.grouping_cols = grouping_cols
        self.driver_col = driver_col
        self.filtered_dataframe: Optional[pl.DataFrame] = None

        self.columnconfigure(0, weight=1)
        self.columnconfigure(1, weight=3)
        self.rowconfigure(0, weight=1)

        # Matplotlib Figure and Canvas
        self.plot_figure, self.ax = plt.subplots(
            figsize=config.PLOT_FIGSIZE, dpi=config.PLOT_DPI
        )
        self.plot_canvas: Optional[FigureCanvasTkAgg] = None

        # UI State Variables
        self.plot_mode = tk.StringVar(value="Unfiltered")
        self.plot_totals = tk.BooleanVar(value=False)
        self.show_threshold = tk.BooleanVar(value=False)
        self.threshold_var = tk.StringVar()
        default_threshold = config.DRIVER_THRESHOLDS.get(self.driver_col, 1000.0)
        self.threshold_var.set(str(default_threshold))

        # Plot customization settings - separate colors for unfiltered/filtered/summed
        self.plot_settings = {
            "plot_type": "line",
            "unfiltered_color": config.PLOT_COLOR_MAP["Ocean Blue"],
            "filtered_color": config.PLOT_COLOR_MAP["Tangerine"],
            "summed_unfiltered_color": config.PLOT_COLOR_MAP["Forest Green"],
            "summed_filtered_color": config.PLOT_COLOR_MAP["Emerald Green"],
            "marker": "o",
            "linewidth": 2,
            "markersize": 6,
            "grid": True,
        }

        # Threshold settings
        self.threshold_settings = {
            "threshold_color": config.PLOT_COLOR_MAP["Crimson"],
            "threshold_linestyle": ":",
            "threshold_linewidth": 2,
        }

        self.entry_labels = self._generate_entry_labels()

        self._create_controls()
        self._create_plot_area()
        
        # Initial plot
        if self.entry_labels:
            self._refresh_plot()

    def _is_month_column(self, col_name: str) -> bool:
        """
        Determine if a column name represents a month period (YYYY-MM format).
        
        This helper prevents non-temporal columns (like rate descriptors) from being
        incorrectly identified as data columns during plotting operations.
        
        Args:
            col_name: Column name to check
        
        Returns:
            True if the column matches YYYY-MM format, False otherwise
        """
        month_pattern = re.compile(r'^\d{4}-\d{2}$')
        return bool(month_pattern.match(col_name))

    def _get_month_columns(self, df: pl.DataFrame) -> List[str]:
        """
        Extract only the month columns from a DataFrame.
        
        Args:
            df: DataFrame to extract month columns from
            
        Returns:
            List of month column names in YYYY-MM format
        """
        exclude_cols = self.grouping_cols + ["ID", "Total"]
        # Also exclude any rate columns that might be present
        rate_cols = [col for col in df.columns if "rate" in col.lower() or "Rate" in col]
        exclude_cols.extend(rate_cols)
        
        month_cols = [
            col for col in df.columns 
            if col not in exclude_cols and self._is_month_column(col)
        ]
        return sorted(month_cols)

    def _generate_entry_labels(self) -> List[str]:
        """Generate descriptive labels for each data entry"""
        df_no_total = self.dataframe.filter(pl.col(self.grouping_cols[0]) != "GRAND TOTAL")
        labels = []
        for row in df_no_total.select(self.grouping_cols).iter_rows():
            label_parts = [str(part) for part in row if part is not None]
            labels.append(" | ".join(label_parts) if label_parts else "Entry")
        return labels

    def _create_controls(self):
        """Create UI controls for entry selection, filtering, and plotting options"""
        # --- Left Panel: Entry Selection ---
        left_panel = ttk.Frame(self)
        left_panel.grid(row=0, column=0, sticky="nswe", padx=(0, 10))
        left_panel.rowconfigure(1, weight=1)
        left_panel.columnconfigure(0, weight=1)

        # Entry selection listbox
        entry_frame = ttk.LabelFrame(left_panel, text="Select Entries", padding=10)
        entry_frame.grid(row=0, column=0, sticky="nswe", pady=(0, 10))
        entry_frame.rowconfigure(0, weight=1)
        entry_frame.columnconfigure(0, weight=1)

        self.entry_listbox = Listbox(
            entry_frame,
            selectmode=tk.MULTIPLE,
            exportselection=False,
            font=config.FONT_CONFIG.get("entry", ("Arial", 9)),
        )
        self.entry_listbox.grid(row=0, column=0, sticky="nswe")
        self.entry_listbox.bind("<<ListboxSelect>>", lambda e: self._on_selection_change())
        
        for label in self.entry_labels:
            self.entry_listbox.insert(tk.END, label)

        listbox_scroll = ttk.Scrollbar(
            entry_frame, orient="vertical", command=self.entry_listbox.yview
        )
        listbox_scroll.grid(row=0, column=1, sticky="ns")
        self.entry_listbox.config(yscrollcommand=listbox_scroll.set)

        # --- Right Panel: Plot Controls ---
        right_panel = ttk.Frame(self)
        right_panel.grid(row=0, column=1, sticky="nswe")
        right_panel.rowconfigure(1, weight=1)
        right_panel.columnconfigure(0, weight=1)

        # Top control bar with clear data mode selection
        control_bar = ttk.Frame(right_panel)
        control_bar.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        control_bar.columnconfigure(4, weight=1)
        
        # Data Mode Selection - Prominent radio buttons
        mode_frame = ttk.LabelFrame(control_bar, text="Data Mode", padding=8)
        mode_frame.grid(row=0, column=0, sticky="w", padx=(0, 10))
        
        mode_inner = ttk.Frame(mode_frame)
        mode_inner.pack()
        
        modes = [("Unfiltered", "Unfiltered"), ("Filtered", "Filtered"), ("Both", "Both")]
        for text, mode in modes:
            ttk.Radiobutton(
                mode_inner, text=text, variable=self.plot_mode, value=mode,
                command=self._refresh_plot
            ).pack(side=tk.LEFT, padx=8)

        # Plot Totals Toggle - Prominent checkbox
        totals_frame = ttk.LabelFrame(control_bar, text="Aggregation", padding=8)
        totals_frame.grid(row=0, column=1, sticky="w", padx=(0, 10))
        
        ttk.Checkbutton(
            totals_frame,
            text="Plot sum of all entries",
            variable=self.plot_totals,
            command=self._refresh_plot
        ).pack()

        # Totals Display Box
        self.totals_display_frame = ttk.LabelFrame(control_bar, text="Totals", padding=8)
        self.totals_display_frame.grid(row=0, column=2, sticky="w", padx=(0, 10))
        
        self.totals_label = ttk.Label(
            self.totals_display_frame, 
            text="No data plotted",
            font=config.FONT_CONFIG.get("small_info", ("Arial", 9))
        )
        self.totals_label.pack()

        # Action buttons
        action_frame = ttk.Frame(control_bar)
        action_frame.grid(row=0, column=4, sticky="e")
        
        ttk.Button(
            action_frame, 
            text="🔄 Refresh Plot", 
            command=self._refresh_plot
        ).pack(side=tk.LEFT, padx=5)

        ttk.Button(
            action_frame,
            text="📊 Set Threshold",
            command=self._open_threshold_dialog
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            action_frame,
            text="⚙️ Plot Options",
            command=self._open_plot_options_dialog
        ).pack(side=tk.LEFT, padx=5)

    def _open_threshold_dialog(self):
        """Open a popup dialog for threshold settings"""
        dialog = Toplevel(self)
        dialog.title("Threshold Settings")
        dialog.geometry("400x350")
        dialog.resizable(False, False)
        
        # Make dialog modal
        dialog.transient(self)
        dialog.grab_set()
        
        # Center the dialog
        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() // 2) - (dialog.winfo_width() // 2)
        y = (dialog.winfo_screenheight() // 2) - (dialog.winfo_height() // 2)
        dialog.geometry(f"+{x}+{y}")
        
        # Main container with padding
        main_frame = ttk.Frame(dialog, padding=20)
        main_frame.pack(fill="both", expand=True)
        
        # Threshold Value
        threshold_value_frame = ttk.Frame(main_frame)
        threshold_value_frame.pack(fill="x", pady=10)
        ttk.Label(threshold_value_frame, text="Monthly Capacity:", width=20).pack(side=tk.LEFT)
        threshold_entry = ttk.Entry(
            threshold_value_frame, 
            textvariable=self.threshold_var, 
            width=15
        )
        threshold_entry.pack(side=tk.LEFT, padx=5)

        # Show Threshold Toggle
        show_threshold_frame = ttk.Frame(main_frame)
        show_threshold_frame.pack(fill="x", pady=10)
        ttk.Checkbutton(
            show_threshold_frame,
            text="Show threshold line on plot",
            variable=self.show_threshold
        ).pack(anchor="w")

        # Threshold Color
        threshold_color_frame = ttk.Frame(main_frame)
        threshold_color_frame.pack(fill="x", pady=10)
        ttk.Label(threshold_color_frame, text="Threshold Color:", width=20).pack(side=tk.LEFT)
        threshold_color_combo = ttk.Combobox(
            threshold_color_frame,
            values=list(config.PLOT_COLOR_MAP.keys()),
            state="readonly",
            width=15
        )
        default_threshold_color = next(
            (k for k, v in config.PLOT_COLOR_MAP.items() if v == self.threshold_settings["threshold_color"]),
            list(config.PLOT_COLOR_MAP.keys())[3],
        )
        threshold_color_combo.set(default_threshold_color)
        threshold_color_combo.pack(side=tk.LEFT, padx=5)

        # Threshold Line Style
        threshold_linestyle_frame = ttk.Frame(main_frame)
        threshold_linestyle_frame.pack(fill="x", pady=10)
        ttk.Label(threshold_linestyle_frame, text="Threshold Line Style:", width=20).pack(side=tk.LEFT)
        threshold_linestyle_combo = ttk.Combobox(
            threshold_linestyle_frame,
            values=[":", "--", "-", "-."],
            state="readonly",
            width=15
        )
        threshold_linestyle_combo.set(self.threshold_settings["threshold_linestyle"])
        threshold_linestyle_combo.pack(side=tk.LEFT, padx=5)

        # Threshold Line Width
        threshold_lw_frame = ttk.Frame(main_frame)
        threshold_lw_frame.pack(fill="x", pady=10)
        ttk.Label(threshold_lw_frame, text="Threshold Line Width:", width=20).pack(side=tk.LEFT)
        threshold_linewidth_combo = ttk.Combobox(
            threshold_lw_frame,
            values=["1", "2", "3", "4", "5"],
            state="readonly",
            width=15
        )
        threshold_linewidth_combo.set(str(self.threshold_settings["threshold_linewidth"]))
        threshold_linewidth_combo.pack(side=tk.LEFT, padx=5)

        # Button frame
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill="x", pady=20)
        
        def apply_and_close():
            try:
                threshold = float(self.threshold_var.get())
            except (ValueError, TypeError):
                messagebox.showerror("Invalid Input", "Threshold must be a valid number.", parent=dialog)
                return

            self.threshold_settings = {
                "threshold_color": config.PLOT_COLOR_MAP.get(
                    threshold_color_combo.get(), 
                    config.PLOT_COLOR_MAP["Crimson"]
                ),
                "threshold_linestyle": threshold_linestyle_combo.get(),
                "threshold_linewidth": int(threshold_linewidth_combo.get()),
            }
            self._refresh_plot()
            dialog.destroy()
        
        ttk.Button(
            button_frame, 
            text="Apply & Close", 
            command=apply_and_close
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            button_frame, 
            text="Cancel", 
            command=dialog.destroy
        ).pack(side=tk.LEFT, padx=5)

    def _open_plot_options_dialog(self):
        """Open a popup dialog for plot customization options"""
        dialog = Toplevel(self)
        dialog.title("Plot Customization")
        dialog.geometry("450x550")
        dialog.resizable(False, False)
        
        # Make dialog modal
        dialog.transient(self)
        dialog.grab_set()
        
        # Center the dialog
        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() // 2) - (dialog.winfo_width() // 2)
        y = (dialog.winfo_screenheight() // 2) - (dialog.winfo_height() // 2)
        dialog.geometry(f"+{x}+{y}")
        
        # Main container with scrollbar
        main_frame = ttk.Frame(dialog)
        main_frame.pack(fill="both", expand=True)
        
        # Create canvas and scrollbar
        canvas = tk.Canvas(main_frame, highlightthickness=0)
        scrollbar = ttk.Scrollbar(main_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas, padding=20)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # Enable mousewheel scrolling
        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        
        canvas.bind_all("<MouseWheel>", _on_mousewheel)
        
        # Unbind when dialog closes
        def on_close():
            canvas.unbind_all("<MouseWheel>")
            dialog.destroy()
        
        dialog.protocol("WM_DELETE_WINDOW", on_close)
        
        canvas.pack(side=tk.LEFT, fill="both", expand=True)
        scrollbar.pack(side=tk.RIGHT, fill="y")

        # Plot Type
        type_frame = ttk.Frame(scrollable_frame)
        type_frame.pack(fill="x", pady=8)
        ttk.Label(type_frame, text="Plot Type:", width=22).pack(side=tk.LEFT)
        plot_type_combo = ttk.Combobox(
            type_frame,
            values=["line", "bar", "scatter", "step"],
            state="readonly",
            width=15
        )
        plot_type_combo.set(self.plot_settings["plot_type"])
        plot_type_combo.pack(side=tk.LEFT, padx=5)

        # Unfiltered Color
        unfiltered_color_frame = ttk.Frame(scrollable_frame)
        unfiltered_color_frame.pack(fill="x", pady=8)
        ttk.Label(unfiltered_color_frame, text="Unfiltered Color:", width=22).pack(side=tk.LEFT)
        unfiltered_color_combo = ttk.Combobox(
            unfiltered_color_frame,
            values=list(config.PLOT_COLOR_MAP.keys()),
            state="readonly",
            width=15
        )
        default_unfiltered_color = next(
            (k for k, v in config.PLOT_COLOR_MAP.items() if v == self.plot_settings["unfiltered_color"]),
            list(config.PLOT_COLOR_MAP.keys())[0],
        )
        unfiltered_color_combo.set(default_unfiltered_color)
        unfiltered_color_combo.pack(side=tk.LEFT, padx=5)

        # Filtered Color
        filtered_color_frame = ttk.Frame(scrollable_frame)
        filtered_color_frame.pack(fill="x", pady=8)
        ttk.Label(filtered_color_frame, text="Filtered Color:", width=22).pack(side=tk.LEFT)
        filtered_color_combo = ttk.Combobox(
            filtered_color_frame,
            values=list(config.PLOT_COLOR_MAP.keys()),
            state="readonly",
            width=15
        )
        default_filtered_color = next(
            (k for k, v in config.PLOT_COLOR_MAP.items() if v == self.plot_settings["filtered_color"]),
            list(config.PLOT_COLOR_MAP.keys())[1],
        )
        filtered_color_combo.set(default_filtered_color)
        filtered_color_combo.pack(side=tk.LEFT, padx=5)

        # Summed Unfiltered Color
        summed_unfiltered_color_frame = ttk.Frame(scrollable_frame)
        summed_unfiltered_color_frame.pack(fill="x", pady=8)
        ttk.Label(summed_unfiltered_color_frame, text="Sum Unfiltered Color:", width=22).pack(side=tk.LEFT)
        summed_unfiltered_color_combo = ttk.Combobox(
            summed_unfiltered_color_frame,
            values=list(config.PLOT_COLOR_MAP.keys()),
            state="readonly",
            width=15
        )
        default_summed_unfiltered_color = next(
            (k for k, v in config.PLOT_COLOR_MAP.items() if v == self.plot_settings["summed_unfiltered_color"]),
            list(config.PLOT_COLOR_MAP.keys())[5],
        )
        summed_unfiltered_color_combo.set(default_summed_unfiltered_color)
        summed_unfiltered_color_combo.pack(side=tk.LEFT, padx=5)

        # Summed Filtered Color
        summed_filtered_color_frame = ttk.Frame(scrollable_frame)
        summed_filtered_color_frame.pack(fill="x", pady=8)
        ttk.Label(summed_filtered_color_frame, text="Sum Filtered Color:", width=22).pack(side=tk.LEFT)
        summed_filtered_color_combo = ttk.Combobox(
            summed_filtered_color_frame,
            values=list(config.PLOT_COLOR_MAP.keys()),
            state="readonly",
            width=15
        )
        default_summed_filtered_color = next(
            (k for k, v in config.PLOT_COLOR_MAP.items() if v == self.plot_settings["summed_filtered_color"]),
            list(config.PLOT_COLOR_MAP.keys())[9],
        )
        summed_filtered_color_combo.set(default_summed_filtered_color)
        summed_filtered_color_combo.pack(side=tk.LEFT, padx=5)

        # Line Width
        lw_frame = ttk.Frame(scrollable_frame)
        lw_frame.pack(fill="x", pady=8)
        ttk.Label(lw_frame, text="Line Width:", width=22).pack(side=tk.LEFT)
        linewidth_combo = ttk.Combobox(
            lw_frame,
            values=["1", "2", "3", "4", "5"],
            state="readonly",
            width=15
        )
        linewidth_combo.set(str(self.plot_settings["linewidth"]))
        linewidth_combo.pack(side=tk.LEFT, padx=5)

        # Marker
        marker_frame = ttk.Frame(scrollable_frame)
        marker_frame.pack(fill="x", pady=8)
        ttk.Label(marker_frame, text="Marker:", width=22).pack(side=tk.LEFT)
        marker_combo = ttk.Combobox(
            marker_frame,
            values=["o", "s", "D", "^", "v", "None"],
            state="readonly",
            width=15
        )
        marker_combo.set(str(self.plot_settings["marker"]))
        marker_combo.pack(side=tk.LEFT, padx=5)

        # Marker Size
        ms_frame = ttk.Frame(scrollable_frame)
        ms_frame.pack(fill="x", pady=8)
        ttk.Label(ms_frame, text="Marker Size:", width=22).pack(side=tk.LEFT)
        markersize_combo = ttk.Combobox(
            ms_frame,
            values=["4", "6", "8", "10", "12"],
            state="readonly",
            width=15
        )
        markersize_combo.set(str(self.plot_settings["markersize"]))
        markersize_combo.pack(side=tk.LEFT, padx=5)

        # Grid
        grid_frame = ttk.Frame(scrollable_frame)
        grid_frame.pack(fill="x", pady=8)
        grid_var = tk.BooleanVar(value=self.plot_settings["grid"])
        ttk.Checkbutton(
            grid_frame,
            text="Show Grid",
            variable=grid_var
        ).pack(anchor="w")

        # Button frame
        button_frame = ttk.Frame(scrollable_frame)
        button_frame.pack(fill="x", pady=20)
        
        def apply_and_close():
            marker = marker_combo.get()
            self.plot_settings = {
                "plot_type": plot_type_combo.get(),
                "unfiltered_color": config.PLOT_COLOR_MAP.get(
                    unfiltered_color_combo.get(), 
                    config.PLOT_COLOR_MAP["Ocean Blue"]
                ),
                "filtered_color": config.PLOT_COLOR_MAP.get(
                    filtered_color_combo.get(), 
                    config.PLOT_COLOR_MAP["Tangerine"]
                ),
                "summed_unfiltered_color": config.PLOT_COLOR_MAP.get(
                    summed_unfiltered_color_combo.get(), 
                    config.PLOT_COLOR_MAP["Forest Green"]
                ),
                "summed_filtered_color": config.PLOT_COLOR_MAP.get(
                    summed_filtered_color_combo.get(), 
                    config.PLOT_COLOR_MAP["Emerald Green"]
                ),
                "marker": None if marker == "None" else marker,
                "linewidth": int(linewidth_combo.get()),
                "markersize": int(markersize_combo.get()),
                "grid": grid_var.get(),
            }
            self._refresh_plot()
            on_close()
        
        ttk.Button(
            button_frame, 
            text="Apply & Close", 
            command=apply_and_close
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            button_frame, 
            text="Cancel", 
            command=on_close
        ).pack(side=tk.LEFT, padx=5)

    def _on_selection_change(self):
        """Handle listbox selection changes"""
        if not self.plot_totals.get():
            self._refresh_plot()

    def _create_plot_area(self):
        """Create the Matplotlib canvas"""
        self.plot_container = ttk.Frame(self)
        self.plot_container.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=10)
        self.plot_container.columnconfigure(0, weight=1)
        self.plot_container.rowconfigure(0, weight=1)

        self.plot_canvas = FigureCanvasTkAgg(self.plot_figure, self.plot_container)
        self.plot_canvas.get_tk_widget().grid(row=0, column=0, sticky="nsew")

    def _update_totals_display(self, totals_info: Dict[str, float]):
        """Update the totals display box with current plot totals"""
        if not totals_info:
            self.totals_label.config(text="No data plotted")
            return
        
        display_lines = []
        for key, value in totals_info.items():
            display_lines.append(f"{key}: {value:,.2f}")
        
        display_text = "\n".join(display_lines)
        self.totals_label.config(text=display_text)

    def _refresh_plot(self):
        """Generate and display the plot based on current UI settings"""
        try:
            threshold = float(self.threshold_var.get())
        except (ValueError, TypeError):
            messagebox.showerror("Invalid Input", "Threshold must be a valid number.")
            return

        selected_indices = self.entry_listbox.curselection()
        mode = self.plot_mode.get()
        show_totals = self.plot_totals.get()
        show_threshold_line = self.show_threshold.get()
        self.ax.cla()

        df_plot = self.dataframe.filter(pl.col(self.grouping_cols[0]) != "GRAND TOTAL")
        
        # Use the new method to get month columns
        month_cols = self._get_month_columns(df_plot)

        if not month_cols:
            self.ax.text(
                0.5, 0.5, 
                "No monthly data columns found to plot.", 
                ha='center', va='center', 
                transform=self.ax.transAxes,
                fontsize=12
            )
            self.plot_figure.tight_layout()
            self.plot_canvas.draw()
            self._update_totals_display({})
            return

        # Calculate filtered data if needed
        if mode in ["Filtered", "Both"] or (show_totals and mode != "Unfiltered"):
            self.filtered_dataframe = smooth_data_with_stockpile(
                df=df_plot.select(self.grouping_cols + month_cols),
                grouping_cols=self.grouping_cols,
                driver_threshold=threshold,
            )
        else:
            self.filtered_dataframe = None

        # Track totals for display
        totals_info = {}

        title = []
        if show_totals:
            totals_info = self._plot_totals(df_plot, month_cols, mode)
            title.append("Total Driver Profile")
        elif selected_indices:
            totals_info = self._plot_selected_entries(df_plot, month_cols, mode, selected_indices)
            title.append("Driver Profile for Selected Entries")
        else:
            self.ax.text(
                0.5, 0.5, 
                "Select entries or toggle 'Plot sum of all entries' to view data.", 
                ha='center', va='center', 
                transform=self.ax.transAxes,
                fontsize=12
            )

        # Plot threshold line if enabled
        if show_threshold_line and (selected_indices or show_totals):
            self.ax.axhline(
                y=threshold, 
                color=self.threshold_settings["threshold_color"], 
                linestyle=self.threshold_settings["threshold_linestyle"], 
                linewidth=self.threshold_settings["threshold_linewidth"], 
                label=f'Threshold',
                alpha=0.7
            )

        if title:
            self.ax.set_title("\n".join(title), loc="left", fontsize=12, fontweight='bold')
        self.ax.set_ylabel(self.driver_col, fontsize=10)
        
        if self.plot_settings["grid"]:
            self.ax.grid(True, which="both", linestyle="--", linewidth=0.5, alpha=0.4)
        
        if selected_indices or show_totals:
            self.ax.legend(fontsize=8, loc='best')
        
        plt.setp(self.ax.get_xticklabels(), rotation=45, ha="right", fontsize=8)
        self.plot_figure.tight_layout()
        self.plot_canvas.draw()

        # Update totals display
        self._update_totals_display(totals_info)

    def _plot_totals(self, df_plot: pl.DataFrame, month_cols: List[str], mode: str) -> Dict[str, float]:
        """Plot total sums across all entries. Returns dict of totals."""
        plot_style = {
            "linewidth": self.plot_settings["linewidth"],
            "marker": self.plot_settings["marker"],
            "markersize": self.plot_settings["markersize"],
        }
        
        totals_info = {}
        
        if mode in ["Unfiltered", "Both"]:
            # Safely extract and sum monthly data, handling None values
            monthly_sums = []
            for month_col in month_cols:
                col_data = df_plot[month_col]
                # Filter out None values and convert to float
                valid_data = [float(x) for x in col_data if x is not None]
                monthly_sums.append(sum(valid_data) if valid_data else 0.0)
            
            grand_total_unfiltered = sum(monthly_sums)
            totals_info["Unfiltered Total"] = grand_total_unfiltered
            
            self.ax.plot(
                month_cols, monthly_sums, 
                color=self.plot_settings["summed_unfiltered_color"], 
                label='Total Unfiltered',
                linestyle=':' if mode == "Both" else '-',
                **plot_style
            )

        if mode in ["Filtered", "Both"] and self.filtered_dataframe is not None:
            # Use the same method to get month columns from filtered data
            f_months = self._get_month_columns(self.filtered_dataframe)
            
            # Safely extract and sum monthly data from filtered dataframe
            filtered_monthly_sums = []
            for month_col in f_months:
                col_data = self.filtered_dataframe[month_col]
                # Filter out None values and convert to float
                valid_data = [float(x) for x in col_data if x is not None]
                filtered_monthly_sums.append(sum(valid_data) if valid_data else 0.0)
            
            grand_total_filtered = sum(filtered_monthly_sums)
            totals_info["Filtered Total"] = grand_total_filtered
            
            self.ax.plot(
                f_months, filtered_monthly_sums, 
                color=self.plot_settings["summed_filtered_color"],
                label='Total Filtered',
                linestyle='-',
                **plot_style
            )

        return totals_info

    def _plot_selected_entries(self, df_plot: pl.DataFrame, month_cols: List[str], mode: str, indices: tuple) -> Dict[str, float]:
        """Plot individual selected entries. Returns dict of totals."""
        colors = list(config.PLOT_COLOR_MAP.values())
        totals_info = {}
        
        for i, idx in enumerate(indices):
            label = self.entry_labels[idx]
            # Use different colors for each entry, cycling through available colors
            entry_unfiltered_color = colors[i % len(colors)]
            entry_filtered_color = colors[(i + 3) % len(colors)]  # Offset for visual distinction
            
            plot_style = {
                "linewidth": self.plot_settings["linewidth"],
                "marker": self.plot_settings["marker"],
                "markersize": self.plot_settings["markersize"],
            }
            
            if mode in ["Unfiltered", "Both"]:
                # Safely extract row data, handling None values
                row_data = []
                for month_col in month_cols:
                    value = df_plot[month_col][idx]
                    row_data.append(float(value) if value is not None else 0.0)
                
                entry_total = sum(row_data)
                totals_info[f"{label} (Unfiltered)"] = entry_total
                
                if self.plot_settings["plot_type"] == "line":
                    self.ax.plot(
                        month_cols, row_data, 
                        color=entry_unfiltered_color, 
                        label=f"{label} - Unfiltered",
                        linestyle=':' if mode == "Both" else '-',
                        **plot_style
                    )
                elif self.plot_settings["plot_type"] == "bar":
                    x_pos = range(len(month_cols))
                    self.ax.bar(
                        x_pos, row_data, 
                        color=entry_unfiltered_color, 
                        alpha=0.7, 
                        label=f"{label} - Unfiltered"
                    )
                elif self.plot_settings["plot_type"] == "scatter":
                    self.ax.scatter(
                        range(len(month_cols)), row_data, 
                        color=entry_unfiltered_color, 
                        s=self.plot_settings["markersize"]*20,
                        alpha=0.7, 
                        label=f"{label} - Unfiltered"
                    )
                elif self.plot_settings["plot_type"] == "step":
                    self.ax.step(
                        range(len(month_cols)), row_data,
                        color=entry_unfiltered_color, 
                        where="mid",
                        linewidth=self.plot_settings["linewidth"],
                        label=f"{label} - Unfiltered"
                    )
            
            if mode in ["Filtered", "Both"] and self.filtered_dataframe is not None:
                group_vals = df_plot.select(self.grouping_cols).row(idx)
                filter_expr = pl.all_horizontal([(pl.col(c) == v) for c, v in zip(self.grouping_cols, group_vals)])
                filtered_row = self.filtered_dataframe.filter(filter_expr)
                
                if filtered_row.height > 0:
                    f_months = self._get_month_columns(filtered_row)
                    # Safely extract filtered row data
                    filtered_data = []
                    for month_col in f_months:
                        value = filtered_row[month_col][0]  # First (and should be only) row
                        filtered_data.append(float(value) if value is not None else 0.0)
                    
                    entry_total_filtered = sum(filtered_data)
                    totals_info[f"{label} (Filtered)"] = entry_total_filtered
                    
                    if self.plot_settings["plot_type"] == "line":
                        self.ax.plot(
                            f_months, filtered_data, 
                            color=entry_filtered_color, 
                            label=f"{label} - Filtered",
                            linestyle='-',
                            **plot_style
                        )
                    elif self.plot_settings["plot_type"] == "bar":
                        x_pos = range(len(f_months))
                        self.ax.bar(
                            x_pos, filtered_data, 
                            color=entry_filtered_color, 
                            alpha=0.5, 
                            label=f"{label} - Filtered"
                        )

        return totals_info

    def export_plot(self):
        """Export current plot as PNG file"""
        if not self.plot_figure:
            messagebox.showwarning("No Plot", "No plot available to export.")
            return

        output_dir = config.OUTPUT_DIRECTORY / config.PLOT_OUTPUT_SUBDIR
        output_dir.mkdir(parents=True, exist_ok=True)

        # Generate a descriptive filename
        base_name = "plot"
        if self.plot_totals.get():
            base_name = "total_driver_plot"
        else:
            selections = self.entry_listbox.curselection()
            if len(selections) == 1:
                safe_entry = self.entry_labels[selections[0]][:30].replace(" ", "_").replace("|", "-").replace(":", "")
                base_name = f"plot_{safe_entry}"
            elif len(selections) > 1:
                base_name = "multi_entry_plot"

        filename = f"{base_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        full_path = output_dir / filename

        try:
            self.plot_figure.savefig(full_path, dpi=300, bbox_inches="tight")
            messagebox.showinfo("Plot Exported", f"Plot saved to: {full_path}")
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export plot: {e}")