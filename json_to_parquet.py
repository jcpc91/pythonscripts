import argparse
import traceback
import bigjson
import json
import pyarrow as pa
import pyarrow.parquet as pq

# Diccionario para mapear tipos de JSON Schema a tipos de PyArrow
JSON_TO_PYARROW_TYPES = {
    "string": pa.string(),
    "number": pa.float64(),
    "integer": pa.int64(),
    "boolean": pa.bool_(),
    "null": pa.null() # Aunque los campos pueden ser nullables, "null" como tipo es específico
}

def convert_json_property_to_pyarrow_field(prop_name, prop_details):
    """
    Convierte una propiedad individual de JSON Schema a un campo de PyArrow.
    Maneja tipos simples, objetos anidados (structs) y arrays (listas).
    Todos los campos se crean inicialmente como no anulables aquí;
    la función make_schema_nullable se encargará de la anulabilidad global después.
    """
    json_type_info = prop_details.get("type")

    # Determinar el tipo JSON primario, manejando el caso donde "type" es una lista (ej. ["string", "null"])
    if isinstance(json_type_info, list):
        # Priorizar el primer tipo no nulo para la conversión de PyArrow.
        # La anulabilidad general se maneja por make_schema_nullable.
        json_type = next((t for t in json_type_info if t != "null"), None)
        if json_type is None and "null" in json_type_info: # Si solo es "null" o ["null"]
             pyarrow_type = pa.null()
             return pa.field(prop_name, pyarrow_type, nullable=False) # Nullable se establecerá globalmente
        elif not json_type: # Caso inesperado, ej. lista vacía o solo tipos no mapeados
            print(f"Advertencia: El campo '{prop_name}' tiene una lista de tipos vacía o no reconocida: {json_type_info}. Usando string() por defecto.")
            pyarrow_type = pa.string() # Fallback seguro
            return pa.field(prop_name, pyarrow_type, nullable=False)
    elif isinstance(json_type_info, str):
        json_type = json_type_info
    else: # Tipo no especificado o formato desconocido
        print(f"Advertencia: El campo '{prop_name}' no tiene un 'type' string o lista de strings definido ({json_type_info}). Usando string() por defecto.")
        pyarrow_type = pa.string()
        return pa.field(prop_name, pyarrow_type, nullable=False)


    if json_type == "object":
        if "properties" in prop_details:
            nested_fields = []
            for nested_prop_name, nested_prop_details in prop_details["properties"].items():
                nested_fields.append(convert_json_property_to_pyarrow_field(nested_prop_name, nested_prop_details))
            pyarrow_type = pa.struct(nested_fields)
        else:
            print(f"Advertencia: El campo de objeto '{prop_name}' no tiene 'properties' definidas. Se creará como un struct vacío.")
            pyarrow_type = pa.struct([])
    elif json_type == "array":
        if "items" in prop_details:
            item_details = prop_details["items"]
            if isinstance(item_details, dict):
                value_field = convert_json_property_to_pyarrow_field("item", item_details) # El nombre "item" es un placeholder
                pyarrow_type = pa.list_(value_field.type)
            else:
                print(f"Advertencia: 'items' para el array '{prop_name}' no es un objeto de esquema. Usando list<string> por defecto.")
                pyarrow_type = pa.list_(pa.string())
        else:
            print(f"Advertencia: El campo de array '{prop_name}' no tiene 'items' definidos. Usando list<string> por defecto.")
            pyarrow_type = pa.list_(pa.string())
    else:
        pyarrow_type = JSON_TO_PYARROW_TYPES.get(json_type, pa.string())

    # Corrección: pa.null() debe ser nullable=True inmediatamente.
    if pyarrow_type == pa.null():
        return pa.field(prop_name, pyarrow_type, nullable=True)
    else:
        return pa.field(prop_name, pyarrow_type, nullable=False)


def json_schema_to_pyarrow_schema(json_schema_content):
    """
    Convierte un JSON Schema (ya cargado como un diccionario Python) a un PyArrow Schema.
    """
    if not isinstance(json_schema_content, dict):
        raise ValueError("El contenido del JSON Schema debe ser un diccionario.")

    schema_type = json_schema_content.get("type")
    fields = []

    if schema_type == "object":
        if "properties" not in json_schema_content:
            raise ValueError("El JSON Schema de tipo 'object' no tiene una clave 'properties'.")
        for prop_name, prop_details in json_schema_content["properties"].items():
            fields.append(convert_json_property_to_pyarrow_field(prop_name, prop_details))

    elif schema_type == "array":
        if "items" not in json_schema_content or not isinstance(json_schema_content["items"], dict):
            raise ValueError("El JSON Schema de tipo 'array' debe tener una clave 'items' que sea un objeto.")
        item_schema = json_schema_content["items"]
        if item_schema.get("type") != "object":
            raise ValueError("Los 'items' de un JSON Schema de tipo 'array' deben ser de tipo 'object'.")
        if "properties" not in item_schema:
            raise ValueError("Los 'items' (objeto) del JSON Schema de tipo 'array' no tienen una clave 'properties'.")
        for prop_name, prop_details in item_schema["properties"].items():
            fields.append(convert_json_property_to_pyarrow_field(prop_name, prop_details))
    else:
        raise ValueError(f"El tipo de JSON Schema raíz no soportado: '{schema_type}'. Debe ser 'object' o 'array' (de objetos).")

    return pa.schema(fields)


def make_schema_nullable(schema):
    """
    Recursively sets all fields in a PyArrow schema to nullable=True.
    """
    new_fields = []
    for field in schema:
        current_type = field.type
        
        if pa.types.is_struct(current_type):
            # current_type es pa.StructType. Iterar sobre él da sus campos hijos.
            # make_schema_nullable(current_type) procesará estos campos hijos.
            processed_type = pa.struct(make_schema_nullable(current_type))
        elif pa.types.is_list(current_type):
            # current_type es pa.ListType
            original_value_field = current_type.value_field # Esto es un pa.Field

            # Crear un esquema temporal solo con este original_value_field.
            dummy_schema_for_value_field = pa.schema([original_value_field])
            
            # Llamar recursivamente a make_schema_nullable en este esquema temporal.
            nullable_schema_for_value_field = make_schema_nullable(dummy_schema_for_value_field)

            # Extraer el campo (ahora completamente anulable) para los ítems de la lista
            processed_value_field = nullable_schema_for_value_field.field(original_value_field.name)
            
            # Crear el nuevo tipo de lista usando el processed_value_field.
            processed_type = pa.list_(processed_value_field)
        else:
            processed_type = current_type
        
        new_fields.append(pa.field(field.name, processed_type, nullable=True, metadata=field.metadata))
    
    return pa.schema(new_fields, metadata=schema.metadata if hasattr(schema, 'metadata') else None)


def save_to_json(data, filename):
    """Saves the given data to a JSON file."""
    try:
        with open(filename, 'w') as f:
            if isinstance(data, pa.Schema):
                schema_repr = []
                for field in data:
                    field_meta_repr = None
                    if field.metadata:
                        try:
                            # Intentar decodificar metadatos si son bytes
                            field_meta_repr = {k.decode(): v.decode() for k, v in field.metadata.items()}
                        except Exception:
                             # Si falla, convertir a string como fallback
                            field_meta_repr = str(field.metadata)

                    schema_repr.append({
                        "name": field.name,
                        "type": str(field.type),
                        "nullable": field.nullable,
                        "metadata": field_meta_repr
                    })
                
                schema_metadata_repr = None
                if data.metadata:
                    try:
                        schema_metadata_repr = {k.decode(): v.decode() for k, v in data.metadata.items()}
                    except Exception:
                        schema_metadata_repr = str(data.metadata)

                json.dump({"fields": schema_repr, "metadata": schema_metadata_repr}, f, indent=4)
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
    cleaned_batch = []
    for record in batch:
        cleaned_record = {}
        for field in schema:
            field_name = field.name
            field_type = field.type
            value = record.get(field_name)

            if value == "" and not pa.types.is_string(field_type) and field.nullable:
                cleaned_record[field_name] = None
            elif value == "" and pa.types.is_boolean(field_type) and field.nullable:
                cleaned_record[field_name] = None
            elif pa.types.is_integer(field_type) and isinstance(value, float) and value.is_integer():
                 cleaned_record[field_name] = int(value)
            elif pa.types.is_string(field_type) and isinstance(value, (int, float, bool)):
                cleaned_record[field_name] = str(value)
            # Manejo de nulos explícitos en JSON que deben ser None en Python para PyArrow
            elif value is None and field.nullable:
                cleaned_record[field_name] = None
            # Si el campo no es anulable y el valor es None, puede causar problemas.
            # Esta función asume que make_schema_nullable ya ha sido llamado.
            else:
                cleaned_record[field_name] = value
        cleaned_batch.append(cleaned_record)
    return cleaned_batch


def json_to_parquet(json_file_path, parquet_file_path, schema_file_path, batch_size=1000):
    writer = None
    pyarrow_schema_final = None
    records_processed = 0
    current_batch = []

    try:
        with open(schema_file_path, 'r') as sf:
            json_schema_data = json.load(sf)

        base_pyarrow_schema = json_schema_to_pyarrow_schema(json_schema_data)
        pyarrow_schema_final = make_schema_nullable(base_pyarrow_schema)

        save_to_json(pyarrow_schema_final, 'pyarrow_schema_from_json_schema.json')
        print(f"PyArrow schema generated from {schema_file_path}, made nullable, and saved.")

        with open(json_file_path, 'rb') as f:
            json_data_iterable = bigjson.load(f)
            writer = pq.ParquetWriter(parquet_file_path, pyarrow_schema_final)

            for item in json_data_iterable:
                current_batch.append(item.to_python())
                if len(current_batch) >= batch_size:
                    cleaned_batch = clean_batch_for_pyarrow(current_batch, pyarrow_schema_final)
                    try:
                        table = pa.Table.from_pylist(cleaned_batch, schema=pyarrow_schema_final)
                    except pa.lib.ArrowInvalid as e_arrow:
                        print(f"\nArrowInvalid error processing batch. First erroring record details:")
                        for i, record_in_batch in enumerate(cleaned_batch):
                            try:
                                pa.Table.from_pylist([record_in_batch], schema=pyarrow_schema_final)
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
                        raise e_arrow # Re-raise original batch error

                    writer.write_table(table)
                    records_processed += len(current_batch)
                    print(f"Processed {records_processed} records...")
                    current_batch = []

            if current_batch:
                cleaned_batch = clean_batch_for_pyarrow(current_batch, pyarrow_schema_final)
                try:
                    table = pa.Table.from_pylist(cleaned_batch, schema=pyarrow_schema_final)
                except pa.lib.ArrowInvalid as e_arrow:
                    print(f"\nArrowInvalid error processing final batch. First erroring record details:")
                    for i, record_in_batch in enumerate(cleaned_batch):
                        try:
                            pa.Table.from_pylist([record_in_batch], schema=pyarrow_schema_final)
                        except pa.lib.ArrowInvalid as e_single:
                            print(f"  Problematic record at index {i} in final batch: {json.dumps(record_in_batch, indent=2, ensure_ascii=False)}")
                            print(f"  Error for this record: {e_single}")
                            break
                    raise e_arrow

                writer.write_table(table)
                records_processed += len(current_batch)
                print(f"Processed final batch of {len(current_batch)} records.")

        if records_processed == 0:
            print(f"Warning: No data was processed from '{json_file_path}'. An empty Parquet file may be created if schema was valid.")

        print(f"Successfully converted '{json_file_path}' to '{parquet_file_path}'. Total records: {records_processed}")

    except FileNotFoundError as fnf_error:
        print(f"Error: File not found - {fnf_error}")
    except json.JSONDecodeError as json_error:
        print(f"Error decoding JSON file ('{schema_file_path}' or '{json_file_path}'): {json_error}")
    except ValueError as val_error: # Captura errores de json_schema_to_pyarrow_schema
        print(f"ValueError during schema conversion or processing: {val_error}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        print(f"Length of current_batch at error: {len(current_batch)}")
        if current_batch:
            save_to_json(current_batch, "error_current_batch.json")
        if pyarrow_schema_final:
            save_to_json(pyarrow_schema_final, "error_current_schema.json")
        else:
            print("pyarrow_schema_final was not generated, not saving to JSON.")
        traceback.print_exc()
    finally:
        if writer:
            writer.close()
            print("Parquet writer closed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert a JSON file to Parquet format using a provided JSON Schema.")
    parser.add_argument("json_file", help="Path to the input JSON file.")
    parser.add_argument("parquet_file", help="Path to save the output Parquet file.")
    parser.add_argument("schema_file", help="Path to the JSON Schema file (e.g., shema.json).")
    parser.add_argument("--batch_size", type=int, default=1000, help="Number of records per batch (default: 1000).")

    args = parser.parse_args()

    json_to_parquet(args.json_file, args.parquet_file, args.schema_file, args.batch_size)
