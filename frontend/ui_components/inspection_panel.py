import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
from datetime import datetime

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