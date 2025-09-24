#!/usr/bin/env python
# vab.py

import sys
import os

def main_launcher():
    """
    This is the main entry point for the Visual Asset Browser CLI.

    Its primary job is to correctly configure the Python path to include the 'src'
    directory, ensuring that all modules can be imported reliably. It also
    handles the special 'install' command before loading the main application.
    """
    # --- 1. Path Configuration ---
    # Get the absolute path of the directory containing THIS script (the project root).
    project_root = os.path.dirname(os.path.abspath(__file__))

    # Construct the absolute path to the 'src' directory.
    src_path = os.path.join(project_root, 'src')

    # Add the 'src' directory to the beginning of the Python path.
    # This is the crucial step that makes imports like 'from query_utils...' work.
    if src_path not in sys.path:
        sys.path.insert(0, src_path)

    # --- 2. Handle the Special 'install' Command ---
    # We check for the 'install' command *before* importing the main app.
    # This allows the installer to run even if dependencies like 'torch' or
    # 'transformers' are not yet installed, preventing an ImportError.
    if len(sys.argv) > 1 and sys.argv[1] == "install":
        # Import the installer function only when needed.
        from installer import install_dependencies
        install_dependencies()
        # Exit cleanly after the installation is complete.
        sys.exit(0)

    # --- 3. Run the Main Application ---
    # If the command was not 'install', we can now safely import and run
    # the main CLI logic from src/main.py.
    from main import main
    main()

if __name__ == "__main__":
    main_launcher()