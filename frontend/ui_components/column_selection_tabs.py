import tkinter as tk
from tkinter import ttk, Listbox
from typing import Dict, List, Callable

import config


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