import argparse
import pandas as pd
import bigjson

def json_to_parquet(json_file_path, parquet_file_path):
    """
    Converts a JSON file to Parquet format using bigjson for reading.

    Args:
        json_file_path (str): The path to the input JSON file.
        parquet_file_path (str): The path to save the output Parquet file.
    """
    try:
        data = []
        with open(json_file_path, 'rb') as f: # bigjson expects a binary file handle
            json_data = bigjson.load(f)
            # Assuming the JSON root is an array of objects
            # If it's a single object or different structure, this needs adjustment
            for item in json_data:
                data.append(item.to_python()) # Convert bigjson objects to Python dicts

        if not data:
            print(f"Warning: No data found or JSON structure not an array of objects in '{json_file_path}'")
            # Create an empty parquet file or handle as an error, depending on desired behavior
            df = pd.DataFrame()
        else:
            df = pd.DataFrame(data)

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
