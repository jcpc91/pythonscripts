import argparse
import pandas as pd

def json_to_parquet(json_file_path, parquet_file_path):
    """
    Converts a JSON file to Parquet format.

    Args:
        json_file_path (str): The path to the input JSON file.
        parquet_file_path (str): The path to save the output Parquet file.
    """
    try:
        # Read the JSON file into a pandas DataFrame
        df = pd.read_json(json_file_path)

        # Write the DataFrame to a Parquet file
        df.to_parquet(parquet_file_path, engine='pyarrow')

        print(f"Successfully converted '{json_file_path}' to '{parquet_file_path}'")

    except FileNotFoundError:
        print(f"Error: JSON file not found at '{json_file_path}'")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(description="Convert a JSON file to Parquet format.")
    parser.add_argument("json_file", help="Path to the input JSON file.")
    parser.add_argument("parquet_file", help="Path to save the output Parquet file.")

    args = parser.parse_args()

    # Call the conversion function
    json_to_parquet(args.json_file, args.parquet_file)
