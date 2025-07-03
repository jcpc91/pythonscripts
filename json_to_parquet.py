import argparse
import bigjson
import pyarrow as pa
import pyarrow.parquet as pq

def json_to_parquet(json_file_path, parquet_file_path, batch_size=1000):
    """
    Converts a JSON file to Parquet format using bigjson for reading
    and PyArrow for writing, processing in batches.

    Args:
        json_file_path (str): The path to the input JSON file.
        parquet_file_path (str): The path to save the output Parquet file.
        batch_size (int): The number of records to process in each batch.
    """
    writer = None
    schema = None
    current_batch = []
    records_processed = 0

    try:
        with open(json_file_path, 'rb') as f: # bigjson expects a binary file handle
            json_data = bigjson.load(f)

            # Assuming the JSON root is an array of objects.
            # If it's a single large object or a different structure, this iteration needs adjustment.
            for item in json_data:
                current_batch.append(item.to_python())

                if len(current_batch) >= batch_size:
                    if writer is None:
                        # First batch, infer schema and initialize writer
                        table = pa.Table.from_pylist(current_batch)
                        schema = table.schema
                        writer = pq.ParquetWriter(parquet_file_path, schema)
                        writer.write_table(table)
                    else:
                        # Subsequent batches, use existing schema
                        table = pa.Table.from_pylist(current_batch, schema=schema)
                        writer.write_table(table)

                    records_processed += len(current_batch)
                    print(f"Processed {records_processed} records...")
                    current_batch = []

            # Write any remaining records in the last batch
            if current_batch:
                if writer is None: # Handles cases where total records < batch_size
                    if not current_batch: # No data at all
                         print(f"Warning: No data found in '{json_file_path}'. Empty Parquet file will be created if it wasn't already.")
                         # Ensure an empty parquet file is created with a schema if possible, or handle error
                         # For now, let's assume if no data, no file or an empty one is okay.
                         # If a schema must be defined for an empty file, that's an extra step.
                         # If current_batch is empty and writer is None, we might not even be able to get a schema.
                         # Let's create an empty table with a dummy schema if truly no data.
                         # This part might need refinement based on desired behavior for empty JSON.
                         if not records_processed: # Only print this if nothing was ever processed
                            print("No records to write. Parquet file might be empty or not created.")
                         # To ensure a file is created, we might need to write an empty table.
                         # However, without a schema, this is tricky.
                         # For now, if current_batch is empty and writer is None, we do nothing further here.
                    else: # Data exists, but less than one batch
                        table = pa.Table.from_pylist(current_batch)
                        schema = table.schema
                        writer = pq.ParquetWriter(parquet_file_path, schema)
                        writer.write_table(table)
                else:
                    table = pa.Table.from_pylist(current_batch, schema=schema)
                    writer.write_table(table)
                records_processed += len(current_batch)
                print(f"Processed final batch of {len(current_batch)} records.")

        if records_processed == 0:
            print(f"Warning: No data was processed from '{json_file_path}'. Output Parquet file might be empty or not created as expected.")
            # If you need to guarantee an empty Parquet file with a specific schema even for empty JSON,
            # that would require defining a default schema or handling it differently.
            # For now, if no records, and writer was never initialized, no file is written by ParquetWriter.
            # To ensure an empty file, one could initialize ParquetWriter with a predefined empty schema if no data.
            # This example assumes it's okay if no file is written for an empty JSON array.

        print(f"Successfully converted '{json_file_path}' to '{parquet_file_path}'. Total records: {records_processed}")

    except FileNotFoundError:
        print(f"Error: JSON file not found at '{json_file_path}'")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if writer:
            writer.close()
            print("Parquet writer closed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert a JSON file to Parquet format using PyArrow.")
    parser.add_argument("json_file", help="Path to the input JSON file.")
    parser.add_argument("parquet_file", help="Path to save the output Parquet file.")
    parser.add_argument("--batch_size", type=int, default=1000, help="Number of records per batch (default: 1000).")

    args = parser.parse_args()

    json_to_parquet(args.json_file, args.parquet_file, args.batch_size)
