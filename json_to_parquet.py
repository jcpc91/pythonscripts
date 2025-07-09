import argparse
import traceback
import bigjson
import json
import pyarrow as pa
import pyarrow.parquet as pq

def make_schema_nullable(schema):
    """
    Recursively sets all fields in a PyArrow schema to nullable=True.
    This handles nested structs and lists, ensuring inner types are also nullable.
    """
    new_fields = []
    for field in schema:
        new_type = field.type
        if pa.types.is_struct(field.type):
            # Si es una estructura, procesar recursivamente su esquema anidado
            new_type = pa.struct(make_schema_nullable(field.type))
        elif pa.types.is_list(field.type):
            # Si es una lista, procesar recursivamente su tipo de valor
            # La lista en sí puede ser anulable, y sus elementos también pueden ser anulables
            list_value_type = field.type.value_type
            
            # Crear un esquema dummy para el tipo de valor de la lista para aplicar make_schema_nullable
            # Esto maneja tanto tipos primitivos como complejos dentro de las listas
            dummy_schema_for_value = pa.schema([pa.field("value", list_value_type)])
            nullable_value_schema = make_schema_nullable(dummy_schema_for_value)
            new_list_value_type = nullable_value_schema.field("value").type
            
            new_type = pa.list_(new_list_value_type)
        
        # Para todos los tipos (incluyendo primitivos y tipos anidados modificados),
        # crear un nuevo campo con nullable=True
        new_fields.append(pa.field(field.name, new_type, nullable=True))
    
    return pa.schema(new_fields)


def save_to_json(data, filename):
    """Saves the given data to a JSON file."""
    try:
        with open(filename, 'w') as f:
            # For PyArrow schema, convert to string for serialization
            if isinstance(data, pa.Schema):
                
                json.dump(str(data), f, indent=4)
            else:
                json.dump(data, f, indent=4)
        print(f"Successfully saved data to {filename}")
    except IOError as e:
        print(f"Error saving data to {filename}: {e}")
    except TypeError as e:
        print(f"Error serializing data to JSON for {filename}: {e}. Data type: {type(data)}")
    except Exception as e:
        print(f"An unexpected error occurred while saving to {filename}: {e}")


def json_to_parquet(json_file_path, parquet_file_path, batch_size=1000):
    """
    Converts a JSON file to Parquet format using bigjson for reading
    and PyArrow for writing, processing in batches. Infers schema dynamically
    and then makes all fields nullable.

    Args:
        json_file_path (str): The path to the input JSON file.
        parquet_file_path (str): The path to save the output Parquet file.
        batch_size (int): The number of records to process in each batch.
    """
    writer = None
    inferred_and_nullable_schema = None # Este será el esquema modificado
    current_batch = []
    records_processed = 0

    try:
        with open(json_file_path, 'rb') as f: # bigjson expects a binary file handle
            json_data = bigjson.load(f)

            for item in json_data:
                current_batch.append(item.to_python())

                if len(current_batch) >= batch_size:
                    if writer is None:
                        # Primer lote: inferir esquema y luego hacerlo nullable
                        temp_table = pa.Table.from_pylist(current_batch)
                        inferred_schema = temp_table.schema
                        inferred_and_nullable_schema = make_schema_nullable(inferred_schema)
                        save_to_json(inferred_and_nullable_schema, 'current_schema.json')
                        writer = pq.ParquetWriter(parquet_file_path, inferred_and_nullable_schema)
                    
                    # Usar from_pylist para todos los lotes para un manejo de tipos más robusto
                    table = pa.Table.from_pylist(current_batch, schema=inferred_and_nullable_schema)
                    writer.write_table(table)

                    records_processed += len(current_batch)
                    print(f"Processed {records_processed} records...")
                    current_batch = []

            # Escribir cualquier registro restante en el último lote
            if current_batch:
                if writer is None: # Maneja casos donde el total de registros < batch_size
                    # Si no hay datos previamente procesados, inferir el esquema del lote actual
                    # y luego hacerlo nullable.
                    if not inferred_and_nullable_schema: # Si el esquema no se ha inferido aún (e.g., solo un lote pequeño)
                        temp_table = pa.Table.from_pylist(current_batch)
                        inferred_schema = temp_table.schema
                        inferred_and_nullable_schema = make_schema_nullable(inferred_schema)

                    writer = pq.ParquetWriter(parquet_file_path, inferred_and_nullable_schema)
                
                # Usar from_pylist para el lote final también
                table = pa.Table.from_pylist(current_batch, schema=inferred_and_nullable_schema)
                writer.write_table(table)
                records_processed += len(current_batch)
                print(f"Processed final batch of {len(current_batch)} records.")

        if records_processed == 0:
            print(f"Warning: No data was processed from '{json_file_path}'. An empty Parquet file with a nullable-friendly schema will be created if possible.")
            # If no records, still attempt to create an empty parquet file with the inferred schema
            if inferred_and_nullable_schema and writer is None:
                writer = pq.ParquetWriter(parquet_file_path, inferred_and_nullable_schema)
            # If writer is still None (e.g., no schema could be inferred from empty data), handle it
            elif writer is None:
                print("Error: Could not create an empty Parquet file as no schema could be inferred from empty data.")
                return # Exit if no schema and no writer
            
            # Ensure writer is closed even if no data was written
            if writer:
                writer.close()
                print("Parquet writer closed.")
            exit(1)


        print(f"Successfully converted '{json_file_path}' to '{parquet_file_path}'. Total records: {records_processed}")

    except FileNotFoundError:
        print(f"Error: JSON file not found at '{json_file_path}'")
    except Exception as e:
        print(f"An error occurred: {e}, len(current_batch): {len(current_batch)}")
        save_to_json(current_batch, "error_current_batch.json")
        # Ensure inferred_and_nullable_schema is not None before trying to save it
        if inferred_and_nullable_schema:
            save_to_json(inferred_and_nullable_schema, "error_current_schema.json")
        else:
            print("inferred_and_nullable_schema is None, not saving to JSON.")
        traceback.print_exc()
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
