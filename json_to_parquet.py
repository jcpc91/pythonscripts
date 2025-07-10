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
        current_type = field.type
        
        if pa.types.is_struct(current_type):
            # Si es una estructura, procesar recursivamente su esquema anidado
            processed_type = pa.struct(make_schema_nullable(current_type))
        elif pa.types.is_list(current_type):
            # Si es una lista, procesar recursivamente su tipo de valor
            list_value_type = current_type.value_type
            
            # Recursively make the list's value type nullable
            # Esto maneja casos donde list_value_type podría ser una estructura u otra lista
            # Para tipos primitivos, simplemente se devuelve el tipo con nullable=True
            if pa.types.is_struct(list_value_type) or pa.types.is_list(list_value_type):
                # Para tipos complejos, llamar recursivamente a make_schema_nullable en un esquema dummy
                dummy_schema_for_value = pa.schema([pa.field("value", list_value_type)])
                nullable_value_schema = make_schema_nullable(dummy_schema_for_value)
                processed_list_value_type = nullable_value_schema.field("value").type
            else:
                # Para tipos primitivos, simplemente hacerlo anulable directamente
                processed_list_value_type = list_value_type.with_nullable(True)
            
            processed_type = pa.list_(processed_list_value_type)
        else:
            # Para todos los demás tipos (primitivos), usar el tipo actual
            processed_type = current_type
        
        # Crear un nuevo campo con el tipo potencialmente modificado y establecer explícitamente nullable=True
        new_fields.append(pa.field(field.name, processed_type, nullable=True))
    
    return pa.schema(new_fields)


def save_to_json(data, filename):
    """Saves the given data to a JSON file."""
    try:
        with open(filename, 'w') as f:
            # Para el esquema de PyArrow, convertir a cadena para serialización
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


def clean_batch_for_pyarrow(batch, schema):
    """
    Cleans a batch of records by converting empty strings to None
    for fields that are not of string type, based on the provided PyArrow schema.
    This helps prevent 'Invalid null value' errors.
    """
    cleaned_batch = []
    for record in batch:
        cleaned_record = {}
        for field in schema:
            field_name = field.name
            field_type = field.type
            value = record.get(field_name) # Usar .get() para manejar claves faltantes con gracia

            # Si el valor es una cadena vacía y el tipo de campo NO es cadena,
            # y el campo es anulable, convertirlo a None.
            # Esto es una causa común de "Invalid null value" cuando PyArrow espera un tipo no-string
            # pero obtiene una cadena vacía.
            if value == "" and not pa.types.is_string(field_type) and field.nullable:
                cleaned_record[field_name] = None
            else:
                cleaned_record[field_name] = value
        cleaned_batch.append(cleaned_record)
    return cleaned_batch


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
                    
                    # Limpiar el lote actual antes de pasarlo a PyArrow
                    cleaned_batch = clean_batch_for_pyarrow(current_batch, inferred_and_nullable_schema)
                    table = pa.Table.from_pylist(cleaned_batch, schema=inferred_and_nullable_schema)
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
                
                # Limpiar el lote final antes de pasarlo a PyArrow
                cleaned_batch = clean_batch_for_pyarrow(current_batch, inferred_and_nullable_schema)
                table = pa.Table.from_pylist(cleaned_batch, schema=inferred_and_nullable_schema)
                writer.write_table(table)
                records_processed += len(current_batch)
                print(f"Processed final batch of {len(current_batch)} records.")

        if records_processed == 0:
            print(f"Warning: No data was processed from '{json_file_path}'. An empty Parquet file with a nullable-friendly schema will be created if possible.")
            # Si no hay registros, intentar crear un archivo parquet vacío con el esquema inferido
            if inferred_and_nullable_schema and writer is None:
                writer = pq.ParquetWriter(parquet_file_path, inferred_and_nullable_schema)
            # Si el escritor sigue siendo None (ej., no se pudo inferir el esquema de datos vacíos), manejarlo
            elif writer is None:
                print("Error: No se pudo crear un archivo Parquet vacío ya que no se pudo inferir ningún esquema de datos vacíos.")
                return # Salir si no hay esquema y no hay escritor
            
            # Asegurarse de que el escritor esté cerrado incluso si no se escribieron datos
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
        # Asegurarse de que inferred_and_nullable_schema no sea None antes de intentar guardarlo
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
