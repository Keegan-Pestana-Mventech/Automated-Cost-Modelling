import tkinter as tk
from tkinter import ttk
import os

import config
from .app_state import AppState
from .stages import (
    SheetInputStage,
    ColumnSelectionStage,
    AggregationStage,
    VisualizationStage,
)
from .ui_components import InspectionPanel
from backend.utils import DataFrameInspector


class ApplicationUI:
    """Main application interface for Excel data processing pipeline"""

    def __init__(self, root: tk.Tk):
        self.root = root
        self.state = AppState(config.EXCEL_FILE, config.OUTPUT_DIRECTORY)
        self.inspector = DataFrameInspector()

        # UI Components
        self.stage_label = None
        self.content_frame = None
        self.inspection_panel = None

        # Stage controllers
        self.sheet_input_stage = None
        self.column_selection_stage = None
        self.aggregation_stage = None
        self.visualization_stage = None

        self._setup_main_window()
        self._initialize_stages()
        self._show_sheet_input()

    def _setup_main_window(self):
        """Initialize main window structure"""
        self.root.title("Excel Data Pipeline Processor")
        self.root.geometry(config.WINDOW_GEOMETRY)
        self.root.minsize(*config.WINDOW_MIN_SIZE)
        self.root.configure(bg=config.COLOR_CONFIG["bg_main"])
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
            font=config.FONT_CONFIG["header"],
        ).grid(row=0, column=0, sticky="w")

        self.stage_label = ttk.Label(
            header,
            text="Stage 1: Sheet Input",
            font=config.FONT_CONFIG["stage"],
            foreground=config.COLOR_CONFIG["stage_fg"],
        )
        self.stage_label.grid(row=0, column=1, sticky="e")

        ttk.Label(
            header,
            text=f"File: {os.path.basename(self.state.excel_file)}",
            font=config.FONT_CONFIG["file_path"],
            foreground=config.COLOR_CONFIG["file_path_fg"],
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=2)

    def _initialize_stages(self):
        """Initialize all stage controllers"""
        stage_context = StageContext(
            root=self.root,
            state=self.state,
            inspector=self.inspector,
            content_frame=self.content_frame,
            inspection_panel=self.inspection_panel,
            update_stage_callback=self.update_stage,
            log_callback=self.log_and_display,
        )

        self.sheet_input_stage = SheetInputStage(
            context=stage_context, on_success=self._show_column_selection
        )

        self.column_selection_stage = ColumnSelectionStage(
            context=stage_context,
            on_back=self._show_sheet_input,
            on_proceed=self._show_aggregation_setup,
        )

        self.aggregation_stage = AggregationStage(
            context=stage_context,
            on_back=self._show_column_selection,
            on_success=self._show_plotting,
        )

        self.visualization_stage = VisualizationStage(
            context=stage_context, on_back=self._show_aggregation_setup
        )

    def update_stage(self, stage: int, title: str):
        """Update stage indicator"""
        self.stage_label.config(text=f"Stage {stage}: {title}")
        self.root.title(f"Excel Data Pipeline Processor - {title}")

    def log_and_display(self, message: str):
        """Add to log and update display"""
        self.state.inspection_log += message
        self.inspection_panel.update_log(self.state.inspection_log)

    def _show_sheet_input(self):
        """Show stage 1: Sheet input"""
        self.inspection_panel.grid_remove()
        self.sheet_input_stage.show()

    def _show_column_selection(self):
        """Show stage 2: Column selection"""
        self.inspection_panel.grid()
        self.column_selection_stage.show()

    def _show_aggregation_setup(self):
        """Show stage 3: Aggregation setup"""
        self.inspection_panel.grid_remove()
        self.aggregation_stage.show()

    def _show_plotting(self):
        """Show stage 4: Visualization"""
        self.inspection_panel.grid_remove()
        self.visualization_stage.show()


class StageContext:
    """Shared context for all stages"""

    def __init__(
        self,
        root,
        state,
        inspector,
        content_frame,
        inspection_panel,
        update_stage_callback,
        log_callback,
    ):
        self.root = root
        self.state = state
        self.inspector = inspector
        self.content_frame = content_frame
        self.inspection_panel = inspection_panel
        self.update_stage = update_stage_callback
        self.log = log_callback

    def clear_content(self):
        """Clear main content area"""
        for widget in self.content_frame.winfo_children():
            widget.destroy()


def main():
    root = tk.Tk()
    ApplicationUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()