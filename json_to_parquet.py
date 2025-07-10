import argparse
import traceback
import bigjson
import json
import pyarrow as pa
import pyarrow.parquet as pq

def make_schema_nullable(schema):
    """
    Recursively sets all fields in a PyArrow schema to nullable=True.
    This handles nested structs and lists, ensuring inner types and list elements are also nullable.
    Field metadata and schema metadata are preserved.
    """
    new_fields = []
    for field in schema: # schema is an iterable of pa.Field objects (e.g. pa.Schema)
        current_type = field.type # This is a pa.DataType
        
        if pa.types.is_struct(current_type):
            # current_type is pa.StructType. Iterating over a StructType yields its child Fields.
            # make_schema_nullable(current_type) processes these child Fields.
            # The result of make_schema_nullable(current_type) is a new pa.Schema object
            # where each field of the struct has been made nullable.
            # pa.struct() can take a pa.Schema object (which is an iterable of fields).
            processed_type = pa.struct(make_schema_nullable(current_type))

        elif pa.types.is_list(current_type):
            # current_type is pa.ListType
            # Get the field that defines the list items (element type and nullability)
            original_value_field = current_type.value_field

            # Create a dummy schema containing only this original_value_field.
            # The name of the field in the dummy schema must match how we retrieve it later.
            dummy_schema_for_value_field = pa.schema([original_value_field])
            
            # Recursively call make_schema_nullable on this dummy schema.
            # This will return a new schema where the field corresponding to original_value_field
            # (and its potential nested types) is made fully nullable.
            nullable_schema_for_value_field = make_schema_nullable(dummy_schema_for_value_field)

            # Extract the (now fully nullable) field for the list items
            processed_value_field = nullable_schema_for_value_field.field(original_value_field.name)

            # Create the new list type using the processed value_field.
            # This ensures that the list elements respect the nullability set on processed_value_field.
            processed_type = pa.list_(processed_value_field)
            
        else:
            # For Primitive Types (and other non-struct, non-list types like pa.null())
            processed_type = current_type # Use the original data type
        
        # Create the new field using the (potentially modified) processed_type,
        # explicitly set nullable=True for the field itself, and preserve its metadata.
        new_fields.append(pa.field(field.name, processed_type, nullable=True, metadata=field.metadata))
    
    # Return a new schema with the modified fields, preserving original schema metadata if it exists.
    return pa.schema(new_fields, metadata=schema.metadata if hasattr(schema, 'metadata') else None)


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
                    
                    try:
                        table = pa.Table.from_pylist(cleaned_batch, schema=inferred_and_nullable_schema)
                    except pa.lib.ArrowInvalid as e_arrow:
                        print(f"\n¡Error de PyArrow al procesar el lote! Intentando identificar el registro problemático...")
                        for i, record_in_batch in enumerate(cleaned_batch):
                            try:
                                # Intentar convertir cada registro individualmente
                                pa.Table.from_pylist([record_in_batch], schema=inferred_and_nullable_schema)
                            except pa.lib.ArrowInvalid as e_single_record:
                                print(f"  Registro problemático encontrado en el índice {i} del lote actual:")
                                print(f"  {json.dumps(record_in_batch, indent=2, ensure_ascii=False)}")
                                print(f"  Error específico para este registro: {e_single_record}")
                                print(f"  Intentando identificar el campo problemático dentro del registro...")
                                for field_name_detail, field_value_detail in record_in_batch.items():
                                    try:
                                        if field_name_detail not in inferred_and_nullable_schema.names:
                                            print(f"    Advertencia: El campo '{field_name_detail}' del registro no está en el esquema inferido. Omitiendo análisis detallado para este campo.")
                                            continue

                                        schema_field_detail = inferred_and_nullable_schema.field(field_name_detail)
                                        single_field_schema_detail = pa.schema([schema_field_detail])
                                        pa.Table.from_pylist([{field_name_detail: field_value_detail}], schema=single_field_schema_detail)
                                    except pa.lib.ArrowInvalid as e_field_detail:
                                        print(f"    --> Campo problemático: '{field_name_detail}'")
                                        try:
                                            field_value_str = json.dumps(field_value_detail, ensure_ascii=False)
                                        except TypeError:
                                            field_value_str = str(field_value_detail) # Fallback if not JSON serializable
                                        print(f"        Valor: {field_value_str} (Tipo Python: {type(field_value_detail).__name__})")
                                        print(f"        Definición de campo en esquema: {str(schema_field_detail)}")
                                        print(f"        Error específico del campo: {e_field_detail}")
                                        # No break here, to see all problematic fields in the record if there are multiple
                                print(f"  Fin del análisis detallado de campos para el registro problemático.")
                                break # Detener después de encontrar el primer registro problemático en el LOTE
                        raise e_arrow # Re-lanzar el error original para que se maneje en el bloque outer except

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
                
                try:
                    table = pa.Table.from_pylist(cleaned_batch, schema=inferred_and_nullable_schema)
                except pa.lib.ArrowInvalid as e_arrow:
                    print(f"\n¡Error de PyArrow al procesar el lote final! Intentando identificar el registro problemático...")
                    for i, record_in_batch in enumerate(cleaned_batch):
                        try:
                            # Intentar convertir cada registro individualmente
                            pa.Table.from_pylist([record_in_batch], schema=inferred_and_nullable_schema)
                        except pa.lib.ArrowInvalid as e_single_record:
                            print(f"  Registro problemático encontrado en el índice {i} del lote actual:")
                                print(f"  {json.dumps(record_in_batch, indent=2, ensure_ascii=False)}")
                            print(f"  Error específico para este registro: {e_single_record}")
                                print(f"  Intentando identificar el campo problemático dentro del registro...")
                                for field_name_detail, field_value_detail in record_in_batch.items():
                                    try:
                                        if field_name_detail not in inferred_and_nullable_schema.names:
                                            print(f"    Advertencia: El campo '{field_name_detail}' del registro no está en el esquema inferido. Omitiendo análisis detallado para este campo.")
                                            continue

                                        schema_field_detail = inferred_and_nullable_schema.field(field_name_detail)
                                        single_field_schema_detail = pa.schema([schema_field_detail])
                                        pa.Table.from_pylist([{field_name_detail: field_value_detail}], schema=single_field_schema_detail)
                                    except pa.lib.ArrowInvalid as e_field_detail:
                                        print(f"    --> Campo problemático: '{field_name_detail}'")
                                        try:
                                            field_value_str = json.dumps(field_value_detail, ensure_ascii=False)
                                        except TypeError:
                                            field_value_str = str(field_value_detail) # Fallback if not JSON serializable
                                        print(f"        Valor: {field_value_str} (Tipo Python: {type(field_value_detail).__name__})")
                                        print(f"        Definición de campo en esquema: {str(schema_field_detail)}")
                                        print(f"        Error específico del campo: {e_field_detail}")
                                        # No break here, to see all problematic fields in the record if there are multiple
                                print(f"  Fin del análisis detallado de campos para el registro problemático.")
                                break # Detener después de encontrar el primer registro problemático en el LOTE
                    raise e_arrow # Re-lanzar el error original para que se maneje en el bloque outer except

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
