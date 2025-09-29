from frontend.main_window import ApplicationUI
import tkinter as tk


def main():
    """Initializes and runs the main application GUI."""
    try:
        root = tk.Tk()
        app = ApplicationUI(root)
        root.mainloop()
    except Exception as e:
        # Fallback for critical errors during initialization
        print(f"❌ CRITICAL APPLICATION ERROR: {e}")
        tk.messagebox.showerror(
            "Critical Error",
            f"A critical error occurred and the application must close: {e}",
        )


if __name__ == "__main__":
    print("🚀 Starting Excel Data Pipeline Processor...")
    main()
    print("🛑 Application has been closed.")
