import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import os
from datetime import datetime
from pathlib import Path

import config
from backend import data_loader


class SheetInputStage:
    """Stage 1: Sheet name input with file browser"""

    def __init__(self, context, on_success):
        self.context = context
        self.on_success = on_success

    def show(self):
        """Display the sheet input interface"""
        self.context.clear_content()
        self.context.update_stage(1, "Sheet Input")

        frame = ttk.Frame(self.context.content_frame, padding="20")
        frame.grid(row=0, column=0, sticky="ew", pady=20)
        frame.columnconfigure(1, weight=1)

        self._create_file_selection_row(frame)
        self._create_sheet_name_row(frame)

    def _create_file_selection_row(self, parent):
        """Create file selection controls"""
        ttk.Label(parent, text="Excel File:", font=config.FONT_CONFIG["label"]).grid(
            row=0, column=0, sticky="w", padx=(0, 10), pady=(0, 10)
        )

        file_display = ttk.Label(
            parent,
            text=os.path.basename(self.context.state.excel_file),
            font=config.FONT_CONFIG["file_path"],
            foreground=config.COLOR_CONFIG["link_fg"],
            relief="sunken",
            padding=5,
        )
        file_display.grid(row=0, column=1, sticky="ew", padx=(0, 10), pady=(0, 10))

        ttk.Button(
            parent,
            text="Browse...",
            command=lambda: self._browse_excel_file(file_display),
        ).grid(row=0, column=2, padx=5, pady=(0, 10))

    def _create_sheet_name_row(self, parent):
        """Create sheet name input controls"""
        ttk.Label(parent, text="Sheet Name:", font=config.FONT_CONFIG["label"]).grid(
            row=1, column=0, sticky="w", padx=(0, 10)
        )

        sheet_entry = ttk.Entry(parent, font=config.FONT_CONFIG["entry"], width=40)
        sheet_entry.grid(row=1, column=1, sticky="ew", padx=(0, 10))

        # Pre-populate with default sheet name
        sheet_entry.insert(0, config.DEFAULT_SHEET_NAME)
        sheet_entry.select_range(0, tk.END)
        sheet_entry.bind("<Return>", lambda e: self._load_sheet(sheet_entry.get()))
        sheet_entry.focus()

        ttk.Button(
            parent,
            text="Load Sheet",
            command=lambda: self._load_sheet(sheet_entry.get()),
        ).grid(row=1, column=2, padx=5)

        ttk.Label(
            parent,
            text=f"Default: '{config.DEFAULT_SHEET_NAME}' — Press Enter to use default or type to change",
            font=config.FONT_CONFIG["small_info"],
            foreground=config.COLOR_CONFIG["stage_fg"],
        ).grid(row=2, column=0, columnspan=3, sticky="w", pady=(10, 0))

    def _browse_excel_file(self, file_display_label):
        """Open file dialog to select an Excel file"""
        filetypes = [("Excel files", "*.xlsx *.xls"), ("All files", "*.*")]

        filename = filedialog.askopenfilename(
            title="Select Excel File",
            filetypes=filetypes,
            initialdir=os.path.dirname(self.context.state.excel_file),
        )

        if filename:
            self.context.state.set_excel_file(Path(filename))
            file_display_label.config(text=os.path.basename(filename))
            self.context.log(
                f"[{datetime.now().strftime('%H:%M:%S')}] FILE_CHANGED: {os.path.basename(filename)}\n"
            )

    def _load_sheet(self, sheet_name: str):
        """Load Excel sheet and proceed to column selection"""
        sheet_name = sheet_name.strip()
        if not sheet_name:
            messagebox.showwarning("Input Required", "Please enter a sheet name.")
            return

        if not os.path.exists(self.context.state.excel_file):
            messagebox.showerror(
                "File Not Found",
                f"Excel file not found: {self.context.state.excel_file}",
            )
            return

        try:
            print(f"Loading sheet: '{sheet_name}'...")
            self.context.state.df = data_loader.load_excel(
                self.context.state.excel_file, sheet_name
            )
            self.context.state.sheet_name = sheet_name

            self.context.log(
                self.context.inspector.log_step(
                    self.context.state.df, "SHEET_LOADED", f"Sheet '{sheet_name}'"
                )
            )
            self.context.log(
                self.context.inspector.inspect_dataframe(
                    self.context.state.df, f"Loaded Sheet: {sheet_name}"
                )
            )

            self.on_success()

        except Exception as e:
            messagebox.showerror(
                "Sheet Load Error", f"Failed to load sheet '{sheet_name}': {e}"
            )
