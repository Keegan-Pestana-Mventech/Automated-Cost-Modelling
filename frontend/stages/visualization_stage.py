import tkinter as tk
from tkinter import ttk

import config
from ..ui_components.plot_view import PlotView


class VisualizationStage:
    """Stage 4: Driver profile visualization"""

    def __init__(self, context, on_back):
        self.context = context
        self.on_back = on_back
        self.plot_view = None

    def show(self):
        """Display the visualization interface"""
        self.context.clear_content()
        self.context.update_stage(4, "Driver Profile Visualization")

        # Configure content frame to expand properly
        self.context.content_frame.columnconfigure(0, weight=1)
        self.context.content_frame.rowconfigure(0, weight=1)

        container = ttk.Frame(self.context.content_frame)
        container.grid(row=0, column=0, sticky="nsew")
        container.columnconfigure(0, weight=1)
        container.rowconfigure(0, weight=0)  # Header - fixed height
        container.rowconfigure(1, weight=1)  # Plot view - expands
        container.rowconfigure(2, weight=0)  # Controls - fixed height

        self._create_header(container)
        self._create_plot_view(container)
        self._create_controls(container)

    def _create_header(self, parent):
        """Create stage header"""
        header_frame = ttk.Frame(parent)
        header_frame.grid(row=0, column=0, sticky="ew", pady=(0, 10), padx=10)
        
        ttk.Label(
            header_frame,
            text="Driver Profile Visualization",
            font=config.FONT_CONFIG["plot_header"],
        ).pack(side=tk.LEFT)

    def _create_plot_view(self, parent):
        """Create the plot view component"""
        grouping_cols = (
            self.context.state.selected_columns["location"]
            + self.context.state.selected_columns["activity"]
        )

        self.plot_view = PlotView(
            parent,
            self.context.state.final_dataframe,
            grouping_cols,
            self.context.state.driver_col,
        )
        self.plot_view.grid(row=1, column=0, sticky="nsew", padx=10, pady=(0, 10))

    def _create_controls(self, parent):
        """Create control buttons"""
        controls = ttk.Frame(parent)
        controls.grid(row=2, column=0, pady=(0, 10), padx=10, sticky="ew")

        ttk.Button(
            controls, 
            text="← Back to Aggregation", 
            command=self.on_back
        ).pack(side=tk.LEFT, padx=5)

        ttk.Button(
            controls, 
            text="💾 Export Plot as PNG", 
            command=self.plot_view.export_plot
        ).pack(side=tk.LEFT, padx=5)