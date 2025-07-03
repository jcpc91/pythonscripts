# JSON to Parquet Converter CLI

This is a command-line interface (CLI) tool to convert JSON files to Parquet format.

## Prerequisites

- Python 3.6 or higher
- pip (Python package installer)

## Installation

1.  **Clone the repository (or download the script):**
    ```bash
    # If you have git installed
    git clone <repository_url>
    cd <repository_directory>
    # Otherwise, just download json_to_parquet.py and requirements.txt
    ```

2.  **Install dependencies:**
    Navigate to the directory containing `json_to_parquet.py` and `requirements.txt`, then run:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

To convert a JSON file to Parquet, run the script from your terminal:

```bash
python json_to_parquet.py <input_json_file_path> <output_parquet_file_path>
```

**Arguments:**

*   `<input_json_file_path>`: The path to the JSON file you want to convert.
*   `<output_parquet_file_path>`: The path where the resulting Parquet file will be saved.

**Example:**

```bash
python json_to_parquet.py input.json output.parquet
```

This command will read `input.json` and create `output.parquet` in the same directory.

## How it works

The script uses the `pandas` library to read the JSON data into a DataFrame and then uses `pyarrow` (as the engine for pandas) to write the DataFrame to a Parquet file.

## Error Handling

-   If the input JSON file is not found, an error message will be displayed.
-   Other potential errors during conversion will also be caught and displayed.