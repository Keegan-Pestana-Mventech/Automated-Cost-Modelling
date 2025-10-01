import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
from typing import Dict, List, Callable, Optional
from datetime import datetime

import polars as pl
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import config


class InspectionPanel(ttk.LabelFrame):
    """Bottom panel for displaying validation and inspection logs"""

    def __init__(self, parent):
        super().__init__(parent, text="Validation & Inspection Log", padding="10")
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        self.log_text = scrolledtext.ScrolledText(
            self, height=12, width=100, font=config.FONT_CONFIG["log"], wrap=tk.WORD
        )
        self.log_text.grid(row=0, column=0, sticky="nsew")

        controls = ttk.Frame(self)
        controls.grid(row=1, column=0, sticky="e", pady=5)

        ttk.Button(controls, text="Export Log", command=self._export_log).pack(
            side=tk.RIGHT, padx=5
        )

        ttk.Button(controls, text="Clear Log", command=self._clear_log).pack(
            side=tk.RIGHT, padx=5
        )

    def update_log(self, content: str):
        """Update log display with new content"""
        self.log_text.delete(1.0, tk.END)
        self.log_text.insert(tk.END, content)
        self.log_text.see(tk.END)

    def _clear_log(self):
        """Clear the log display"""
        self.log_text.delete(1.0, tk.END)

    def _export_log(self):
        """Export current log to text file"""
        filename = f"inspection_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        try:
            content = self.log_text.get(1.0, tk.END)
            with open(filename, "w") as f:
                f.write(f"EXCEL DATA PIPELINE - INSPECTION LOG\n{'=' * 70}\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"{'=' * 70}\n\n{content}")
            messagebox.showinfo("Log Exported", f"Log exported to: {filename}")
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export log: {e}")


class ColumnSelectionTabs(ttk.Notebook):
    """Tabbed interface for categorizing columns into location, activity, timing, and drivers"""

    def __init__(self, parent, columns: List[str], on_selection_change: Callable):
        super().__init__(parent)
        self.columns = columns
        self.on_selection_change = on_selection_change
        self.checkboxes: Dict[str, Dict] = {}
        self.tab_widgets: Dict[str, Dict] = {}

        for title, key, desc in config.COLUMN_CATEGORIES:
            tab = self._create_tab(title, key, desc)
            self.add(tab, text=title)

    def _create_tab(self, title: str, key: str, description: str) -> ttk.Frame:
        """Create a single tab with searchable checkboxes"""
        tab = ttk.Frame(self, padding="15")
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(2, weight=1)

        ttk.Label(
            tab,
            text=description,
            font=config.FONT_CONFIG["description"],
            wraplength=800,
        ).grid(row=0, column=0, sticky="ew", pady=(0, 10))

        search_frame = ttk.Frame(tab)
        search_frame.grid(row=1, column=0, sticky="ew", pady=(0, 10))
        ttk.Label(search_frame, text="🔍 Filter:").pack(side=tk.LEFT, padx=(0, 5))

        search_entry = ttk.Entry(search_frame)
        search_entry.pack(side=tk.LEFT, fill=tk.X, expand=True)
        search_entry.bind("<KeyRelease>", lambda e, k=key: self._filter_columns(k))

        canvas = tk.Canvas(tab, highlightthickness=0)
        scrollbar = ttk.Scrollbar(tab, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)

        scrollable_frame.bind(
            "<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.grid(row=2, column=0, sticky="nsew")
        scrollbar.grid(row=2, column=1, sticky="ns")

        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

        canvas.bind("<MouseWheel>", _on_mousewheel)
        scrollable_frame.bind("<MouseWheel>", _on_mousewheel)

        self.tab_widgets[key] = {
            "search_entry": search_entry,
            "scrollable_frame": scrollable_frame,
        }

        self.checkboxes[key] = {}
        num_columns = config.COLUMN_SELECTION_GRID_COLUMNS
        for i, column in enumerate(self.columns):
            var = tk.BooleanVar()
            cb = ttk.Checkbutton(
                scrollable_frame,
                text=column,
                variable=var,
                command=self.on_selection_change,
            )
            row, col = divmod(i, num_columns)
            cb.grid(row=row, column=col, sticky="w", padx=10, pady=3)
            cb.bind("<MouseWheel>", _on_mousewheel)
            self.checkboxes[key][column] = {"var": var, "widget": cb}

        return tab

    def _filter_columns(self, category_key: str):
        """Filter checkboxes based on search term"""
        search_term = self.tab_widgets[category_key]["search_entry"].get().lower()

        visible_columns = [
            col for col in self.checkboxes[category_key] if search_term in col.lower()
        ]

        for data in self.checkboxes[category_key].values():
            data["widget"].grid_remove()

        num_columns = config.COLUMN_SELECTION_GRID_COLUMNS
        for i, column in enumerate(visible_columns):
            widget = self.checkboxes[category_key][column]["widget"]
            row, col = divmod(i, num_columns)
            widget.grid(row=row, column=col, sticky="w", padx=10, pady=3)

    def get_selected_columns(self) -> Dict[str, List[str]]:
        """Return dictionary of selected columns by category"""
        selected = {key: [] for key in self.checkboxes.keys()}
        for category, checkboxes in self.checkboxes.items():
            for column, data in checkboxes.items():
                if data["var"].get():
                    selected[category].append(column)
        return selected

    def get_selection_count(self) -> int:
        """Return total number of selected columns"""
        return sum(
            data["var"].get()
            for cat in self.checkboxes.values()
            for data in cat.values()
        )

    def set_selections(self, selections: Dict[str, List[str]]):
        """Set checkboxes based on a dictionary of pre-selected columns."""
        self.clear_all()
        for category, columns in selections.items():
            if category in self.checkboxes:
                for column in columns:
                    if column in self.checkboxes[category]:
                        self.checkboxes[category][column]["var"].set(True)
        self.on_selection_change()

    def clear_all(self):
        """Deselect all checkboxes"""
        for section in self.checkboxes.values():
            for data in section.values():
                data["var"].set(False)


class PlotView(ttk.Frame):
    """Visualization panel for driver profile plots with customizable settings"""

    def __init__(
        self,
        parent,
        dataframe: pl.DataFrame,
        grouping_cols: List[str],
        driver_col: str,
        plot_generator,
    ):
        super().__init__(parent)
        self.dataframe = dataframe
        self.grouping_cols = grouping_cols
        self.driver_col = driver_col
        self.plot_generator = plot_generator

        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)

        self.entry_labels = self._generate_entry_labels()
        self.plot_figure: Optional[plt.Figure] = None
        self.plot_canvas: Optional[FigureCanvasTkAgg] = None

        self.color_map = config.PLOT_COLOR_MAP
        self.plot_settings = config.DEFAULT_PLOT_SETTINGS.copy()

        self._create_controls()
        self._create_plot_area()

        if self.entry_labels:
            self._generate_plot()

    def _generate_entry_labels(self) -> List[str]:
        """Generate descriptive labels for each data entry"""
        labels = []
        df_no_total = self.dataframe.filter(
            pl.col(self.grouping_cols[0]) != "GRAND TOTAL"
        )
        for i in range(len(df_no_total)):
            label_parts = [
                f"{col}: {df_no_total[col][i]}"
                for col in self.grouping_cols
                if df_no_total[col][i] is not None
            ]
            labels.append(" | ".join(label_parts) if label_parts else f"Entry {i + 1}")
        return labels

    def _create_controls(self):
        """Create dropdowns for entry selection and plot settings"""
        control_frame = ttk.Frame(self)
        control_frame.grid(row=0, column=0, sticky="ew", pady=10)
        control_frame.columnconfigure(1, weight=1)

        ttk.Label(
            control_frame, text="Select Entry:", font=config.FONT_CONFIG["label"]
        ).grid(row=0, column=0, sticky="w", padx=(0, 10), pady=5)

        self.entry_combo = ttk.Combobox(
            control_frame, values=self.entry_labels, state="readonly", width=60
        )
        self.entry_combo.grid(row=0, column=1, sticky="ew", pady=5, padx=(0, 10))
        self.entry_combo.bind("<<ComboboxSelected>>", lambda e: self._generate_plot())

        if self.entry_labels:
            self.entry_combo.set(self.entry_labels[0])

        ttk.Label(
            control_frame, text="Plot Type:", font=config.FONT_CONFIG["label"]
        ).grid(row=0, column=2, sticky="w", padx=(20, 10), pady=5)
        self.plot_type_combo = ttk.Combobox(
            control_frame,
            values=["line", "bar", "scatter", "step"],
            state="readonly",
            width=10,
        )
        self.plot_type_combo.set(self.plot_settings["plot_type"])
        self.plot_type_combo.grid(row=0, column=3, sticky="ew", pady=5, padx=(0, 10))
        self.plot_type_combo.bind(
            "<<ComboboxSelected>>", lambda e: self._update_plot_settings()
        )

        ttk.Label(control_frame, text="Color:", font=config.FONT_CONFIG["label"]).grid(
            row=1, column=0, sticky="w", padx=(0, 10), pady=5
        )
        self.color_combo = ttk.Combobox(
            control_frame,
            values=list(self.color_map.keys()),
            state="readonly",
            width=15,
        )
        # Find key for default color value
        default_color_name = next(
            (k for k, v in self.color_map.items() if v == self.plot_settings["color"]),
            list(self.color_map.keys())[0],
        )
        self.color_combo.set(default_color_name)
        self.color_combo.grid(row=1, column=1, sticky="w", pady=5, padx=(0, 10))
        self.color_combo.bind(
            "<<ComboboxSelected>>", lambda e: self._update_plot_settings()
        )

        ttk.Label(
            control_frame, text="Line Width:", font=config.FONT_CONFIG["label"]
        ).grid(row=1, column=2, sticky="w", padx=(20, 10), pady=5)
        self.linewidth_combo = ttk.Combobox(
            control_frame, values=["1", "2", "3", "4", "5"], state="readonly", width=5
        )
        self.linewidth_combo.set(str(self.plot_settings["linewidth"]))
        self.linewidth_combo.grid(row=1, column=3, sticky="w", pady=5, padx=(0, 10))
        self.linewidth_combo.bind(
            "<<ComboboxSelected>>", lambda e: self._update_plot_settings()
        )

        ttk.Label(control_frame, text="Marker:", font=config.FONT_CONFIG["label"]).grid(
            row=2, column=0, sticky="w", padx=(0, 10), pady=5
        )
        self.marker_combo = ttk.Combobox(
            control_frame,
            values=["o", "s", "D", "^", "v", "None"],
            state="readonly",
            width=8,
        )
        self.marker_combo.set(str(self.plot_settings["marker"]))
        self.marker_combo.grid(row=2, column=1, sticky="w", pady=5, padx=(0, 10))
        self.marker_combo.bind(
            "<<ComboboxSelected>>", lambda e: self._update_plot_settings()
        )

        ttk.Label(
            control_frame, text="Marker Size:", font=config.FONT_CONFIG["label"]
        ).grid(row=2, column=2, sticky="w", padx=(20, 10), pady=5)
        self.markersize_combo = ttk.Combobox(
            control_frame,
            values=["4", "6", "8", "10", "12"],
            state="readonly",
            width=5,
        )
        self.markersize_combo.set(str(self.plot_settings["markersize"]))
        self.markersize_combo.grid(row=2, column=3, sticky="w", pady=5, padx=(0, 10))
        self.markersize_combo.bind(
            "<<ComboboxSelected>>", lambda e: self._update_plot_settings()
        )

        self.grid_var = tk.BooleanVar(value=self.plot_settings["grid"])
        grid_check = ttk.Checkbutton(
            control_frame,
            text="Show Grid",
            variable=self.grid_var,
            command=self._update_plot_settings,
        )
        grid_check.grid(row=2, column=4, sticky="w", padx=(20, 10), pady=5)

    def _update_plot_settings(self):
        """Update plot settings from UI controls"""
        selected_color_name = self.color_combo.get()
        marker = self.marker_combo.get()
        self.plot_settings = {
            "plot_type": self.plot_type_combo.get(),
            "color": self.color_map.get(
                selected_color_name, config.DEFAULT_PLOT_SETTINGS["color"]
            ),
            "marker": None if marker == "None" else marker,
            "linewidth": int(self.linewidth_combo.get()),
            "markersize": int(self.markersize_combo.get()),
            "grid": self.grid_var.get(),
        }
        self._generate_plot()

    def _create_plot_area(self):
        """Create matplotlib canvas for plots"""
        self.plot_container = ttk.Frame(self)
        self.plot_container.grid(row=1, column=0, sticky="nsew", pady=10)
        self.plot_container.columnconfigure(0, weight=1)
        self.plot_container.rowconfigure(0, weight=1)

    def _generate_plot(self):
        """Generate and display the driver profile plot"""
        selected_label = self.entry_combo.get()
        if not selected_label:
            return

        entry_index = self.entry_labels.index(selected_label)

        self.plot_figure = self.plot_generator.generate_plot(
            df=self.dataframe,
            entry_index=entry_index,
            selected_entry_label=selected_label,
            grouping_cols=self.grouping_cols,
            driver_col_name=self.driver_col,
            plot_settings=self.plot_settings,
        )

        if self.plot_canvas:
            self.plot_canvas.get_tk_widget().destroy()

        self.plot_canvas = FigureCanvasTkAgg(self.plot_figure, self.plot_container)
        self.plot_canvas.get_tk_widget().grid(row=0, column=0, sticky="nsew")
        self.plot_canvas.draw()

    def export_plot(self):
        """Export current plot as PNG file to the data/plots directory."""
        if not self.plot_figure:
            messagebox.showwarning("No Plot", "Please generate a plot first.")
            return

        # Define the output directory using Path objects from config
        output_dir = config.OUTPUT_DIRECTORY / config.PLOT_OUTPUT_SUBDIR

        # Create the directory if it doesn't exist
        output_dir.mkdir(parents=True, exist_ok=True)

        safe_entry = (
            self.entry_combo.get()[:30]
            .replace(" ", "_")
            .replace("|", "-")
            .replace(":", "")
        )
        filename = (
            f"driver_plot_{safe_entry}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        )

        # Construct the full file path
        full_path = output_dir / filename

        try:
            self.plot_figure.savefig(full_path, dpi=300, bbox_inches="tight")
            messagebox.showinfo("Plot Exported", f"Plot saved as: {full_path}")
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export plot: {e}")