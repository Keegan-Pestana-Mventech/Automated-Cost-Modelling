import tkinter as tk
from tkinter import ttk, messagebox

import config
from backend import column_generation

class TransformationStage:
    """Stage 3: Create new columns from existing ones"""
        
    def __init__(self, context, on_back, on_proceed):
        self.context = context
        self.on_back = on_back
        self.on_proceed = on_proceed
        self.transformations = []
        # Start with the original DataFrame (complete sheet data)
        self.transformed_df = self.context.state.df

    def show(self):
        """Display the transformation setup interface"""
        self.context.clear_content()
        self.context.update_stage(3, "Column Transformation")

        self.main_frame = ttk.Frame(self.context.content_frame, padding="20")
        self.main_frame.grid(row=0, column=0, sticky="nsew")
        self.main_frame.columnconfigure(0, weight=1)
        self.main_frame.rowconfigure(2, weight=1)

        self._create_header(self.main_frame)
        self._create_input_panel(self.main_frame)
        self._create_transformation_list(self.main_frame)
        self._create_controls(self.main_frame)

    def _create_header(self, parent):
        ttk.Label(
            parent,
            text="Generate New Columns",
            font=config.FONT_CONFIG["plot_header"],
        ).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 15))

    def _create_input_panel(self, parent):
        panel = ttk.LabelFrame(parent, text="New Operation", padding="15")
        panel.grid(row=1, column=0, sticky="ew", pady=(0, 10))
        panel.columnconfigure(1, weight=1)
        
        ttk.Label(panel, text="Target Columns:").grid(row=0, column=0, sticky="w", padx=5, pady=5)
        self.target_cols_listbox = tk.Listbox(panel, selectmode=tk.MULTIPLE, height=5, exportselection=False)
        
        # Use ALL columns from the original DataFrame (complete sheet)
        all_columns = sorted(self.context.state.df.columns)
        
        for col in all_columns:
            self.target_cols_listbox.insert(tk.END, col)
        
        self.target_cols_listbox.grid(row=0, column=1, sticky="ew", padx=5, pady=5)
        
        ttk.Label(panel, text="Operation:").grid(row=1, column=0, sticky="w", padx=5, pady=5)
        self.operation_combo = ttk.Combobox(panel, values=["sum", "multiply", "divide"], state="readonly")
        self.operation_combo.grid(row=1, column=1, sticky="w", padx=5, pady=5)
        self.operation_combo.set("sum")
        
        ttk.Label(panel, text="New Column Name:").grid(row=2, column=0, sticky="w", padx=5, pady=5)
        self.new_col_name_entry = ttk.Entry(panel)
        self.new_col_name_entry.grid(row=2, column=1, sticky="ew", padx=5, pady=5)
        
        add_btn = ttk.Button(panel, text="Add Transformation", command=self._add_transformation)
        add_btn.grid(row=3, column=1, sticky="e", padx=5, pady=10)

    def _create_transformation_list(self, parent):
        list_frame = ttk.LabelFrame(parent, text="Applied Transformations", padding="10")
        list_frame.grid(row=2, column=0, sticky="nsew", pady=10)
        list_frame.columnconfigure(0, weight=1)
        list_frame.rowconfigure(0, weight=1)
        
        self.tree = ttk.Treeview(list_frame, columns=("New Column", "Operation", "Targets"), show="headings", height=5)
        self.tree.heading("New Column", text="New Column")
        self.tree.heading("Operation", text="Operation")
        self.tree.heading("Targets", text="Target Columns")
        self.tree.column("New Column", width=150)
        self.tree.column("Operation", width=100)
        self.tree.grid(row=0, column=0, sticky="nsew")

        scrollbar = ttk.Scrollbar(list_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        scrollbar.grid(row=0, column=1, sticky="ns")
        
        remove_btn = ttk.Button(list_frame, text="Remove Selected", command=self._remove_transformation)
        remove_btn.grid(row=1, column=0, sticky='e', pady=5)

    def _create_controls(self, parent):
        controls = ttk.Frame(parent)
        controls.grid(row=3, column=0, columnspan=2, pady=(20, 0), sticky="ew")
        ttk.Button(controls, text="← Back to Column Selection", command=self.on_back).pack(side=tk.LEFT, padx=5)
        ttk.Button(controls, text="Export Transformed Data", command=self._export_data).pack(side=tk.LEFT, padx=5)
        ttk.Button(controls, text="Proceed to Aggregation →", command=self._proceed).pack(side=tk.LEFT, padx=5)

    def _add_transformation(self):
        selected_indices = self.target_cols_listbox.curselection()
        if not selected_indices:
            messagebox.showerror("Input Error", "Please select at least one target column.")
            return
            
        target_cols = [self.target_cols_listbox.get(i) for i in selected_indices]
        operation = self.operation_combo.get()
        new_col_name = self.new_col_name_entry.get().strip()

        if not new_col_name:
            messagebox.showerror("Input Error", "Please provide a name for the new column.")
            return

        if operation == "divide" and len(target_cols) != 2:
            messagebox.showerror("Input Error", "The 'divide' operation requires exactly two target columns.")
            return
            
        transformation = {"new_col": new_col_name, "op": operation, "targets": target_cols}
        self.transformations.append(transformation)
        self.tree.insert("", "end", values=(new_col_name, operation, ", ".join(target_cols)))
        
        self.new_col_name_entry.delete(0, tk.END)
        self.target_cols_listbox.selection_clear(0, tk.END)
        self.operation_combo.set("sum")
        
        self._reapply_transformations()

    def _remove_transformation(self):
        selected_items = self.tree.selection()
        if not selected_items:
            messagebox.showwarning("No selection", "Please select a transformation to remove.")
            return

        for item in selected_items:
            item_values = self.tree.item(item, 'values')
            self.transformations = [t for t in self.transformations if t['new_col'] != item_values[0]]
            self.tree.delete(item)

        self._reapply_transformations()

    def _reapply_transformations(self):
        # Always start from the original DataFrame and apply all transformations
        temp_df = self.context.state.df.clone()  # Use clone to avoid modifying original
        try:
            for trans in self.transformations:
                temp_df = column_generation.generate_new_column(
                    df=temp_df,
                    target_cols=trans["targets"],
                    new_col_name=trans["new_col"],
                    operation=trans["op"]
                )
            
            self.transformed_df = temp_df
            self.context.log(self.context.inspector.log_step(self.transformed_df, "TRANSFORMATION_APPLIED", "Preview of transformed data generated."))
            self.context.log(self.context.inspector.inspect_dataframe(self.transformed_df, "Preview of Transformed Data"))
            
        except (ValueError, TypeError) as e:
            messagebox.showerror("Transformation Error", f"Could not apply transformation: {e}")
            bad_transform = self.transformations.pop()
            self.tree.delete(self.tree.get_children()[-1])
            self.context.log(f"ERROR: Failed to apply transformation for '{bad_transform['new_col']}'. Reverting.\n")
            self._reapply_transformations()

    def _export_data(self):
        if self.transformed_df is None or self.transformed_df.height == 0:
            messagebox.showwarning("No Data", "There is no data to export.")
            return
        try:
            export_path = self.context.state.export_transformed_data(self.transformed_df)
            messagebox.showinfo("Export Successful", f"Transformed data exported to:\n{export_path}")
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export data: {e}")

    def _proceed(self):
        # Update the state with the fully transformed DataFrame
        self.context.state.df = self.transformed_df
        
        # Add any newly created columns to the drivers list so they're available for aggregation
        for trans in self.transformations:
            new_col = trans["new_col"]
            if new_col not in self.context.state.selected_columns["drivers"]:
                self.context.state.selected_columns["drivers"].append(new_col)
        
        self.on_proceed()