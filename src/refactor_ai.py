import os
import argparse
import time
from pathlib import Path
import google.generativeai as genai
from tqdm import tqdm

# The core prompt that instructs the AI on how to refactor the code.
REFACTORING_PROMPT = """
You are an expert Python programmer and an automated code refactoring tool.
Your task is to take the user's Python code and refactor it to meet the highest standards of quality and readability.

Please apply the following transformations:

1.  **Add Type Hints:** Add PEP 484 type hints to all function signatures (arguments and return types). Use the `typing` module where necessary.
2.  **Generate Docstrings:** Create a comprehensive Google-style docstring for every class and function. The docstring should include:
    - A one-line summary of the object's purpose.
    - A more detailed explanation.
    - An `Args:` section describing each parameter.
    - A `Returns:` section describing the return value.
    - A `Raises:` section if the function explicitly raises exceptions.
3.  **Apply PEP 8 Formatting:** Ensure the code is formatted according to PEP 8 style guidelines. This includes line length, whitespace, and naming conventions. The style should be similar to what the `black` code formatter would produce.
4.  **Improve Readability:**
    - If variable names are unclear (e.g., `lst`, `d`), replace them with more descriptive names.
    - Perform minor simplifications or logical improvements if they make the code clearer without changing its functionality.
5.  **Preserve Logic:** The core functionality and logic of the code MUST remain identical to the original.
6.  **Output Format:** ONLY return the raw, refactored Python code. Do not include any explanations, greetings, or markdown code fences like ```python ... ```. Your entire output should be valid Python source code.

Here is the code to refactor:
---
"""

def configure_api():
    """Configures the Generative AI API with the key from environment variables."""
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        print("Error: GOOGLE_API_KEY environment variable not set.")
        print("Please get an API key from https://aistudio.google.com/app/apikey and set the variable.")
        exit(1)
    genai.configure(api_key=api_key)

def refactor_code(source_code: str, model_name: str) -> str | None:
    """
    Sends the source code to the AI for refactoring.

    Args:
        source_code: A string containing the Python code to refactor.
        model_name: The name of the generative model to use.

    Returns:
        The refactored code as a string, or None if an error occurred.
    """
    try:
        model = genai.GenerativeModel(model_name)
        full_prompt = REFACTORING_PROMPT + source_code
        response = model.generate_content(full_prompt)
        # Clean potential markdown fences from the response
        cleaned_response = response.text.strip().removeprefix("```python").removesuffix("```").strip()
        return cleaned_response
    except Exception as e:
        print(f"\nAn error occurred while communicating with the AI: {e}")
        return None

def process_file(file_path: Path, overwrite: bool, model_name: str):
    """
    Reads a file, sends its content for refactoring, and saves the result.

    Args:
        file_path: The path to the Python file to process.
        overwrite: If True, overwrite the original file. Otherwise, create a new file.
        model_name: The name of the model to use for refactoring.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            original_code = f.read()

        if not original_code.strip():
            return # Skip empty files silently for a cleaner progress bar

        refactored_code = refactor_code(original_code, model_name)

        if refactored_code:
            if overwrite:
                output_path = file_path
            else:
                output_path = file_path.with_name(f"{file_path.stem}_refactored{file_path.suffix}")

            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(refactored_code)
        else:
            # Error message is printed inside refactor_code, so just notify here
            print(f"\nFailed to refactor '{file_path}'. Skipping.")

    except Exception as e:
        print(f"\nAn error occurred while processing file {file_path}: {e}")

def main():
    """Main function to parse arguments and process files."""
    parser = argparse.ArgumentParser(
        description="""Automated Python Code Refactoring using AI.
        This script refactors Python files by adding type hints, docstrings,
        and improving readability according to best practices.""",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "paths",
        nargs='+',
        help="One or more paths to Python files or directories to refactor."
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite the original files. WARNING: Use with caution and version control."
    )
    parser.add_argument(
        "--model",
        default="gemini-pro",
        help="The generative AI model to use. Defaults to 'gemini-pro' for the free tier."
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay in seconds between API calls to avoid rate limiting. Defaults to 1.0."
    )
    args = parser.parse_args()

    configure_api()

    files_to_process = []
    for path_str in args.paths:
        path = Path(path_str)
        if path.is_dir():
            files_to_process.extend(sorted(path.rglob("*.py")))
        elif path.is_file() and path.suffix == '.py':
            files_to_process.append(path)
        else:
            print(f"Warning: '{path}' is not a valid Python file or directory. Skipping.")

    if not files_to_process:
        print("No Python files found to process.")
        return

    print(f"Found {len(files_to_process)} Python file(s) to process.")
    print(f"Using model: {args.model}")
    print(f"Delay between files: {args.delay}s")

    if args.overwrite:
        print("WARNING: --overwrite flag is set. Original files will be modified.")
        if input("Are you sure you want to continue? (y/n): ").lower() != 'y':
            print("Aborting.")
            return

    with tqdm(total=len(files_to_process), desc="Refactoring files", unit="file") as pbar:
        for i, file_path in enumerate(files_to_process):
            process_file(file_path, args.overwrite, args.model)
            pbar.update(1)

            # Throttle the API calls to stay within rate limits
            if i < len(files_to_process) - 1 and args.delay > 0:
                time.sleep(args.delay)

    print("\nRefactoring process completed.")

if __name__ == "__main__":
    main()