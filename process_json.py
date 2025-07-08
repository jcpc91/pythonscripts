import bigjson
import json
import os # Importar para manejo de archivos, por ejemplo, verificar si existe

# Import specific types now that they are known
from bigjson.array import Array as BigJSONArray
from bigjson.obj import Object as BigJSONObject

# La función convert_bigjson_to_native seguiría siendo útil si necesitas convertir
# objetos individuales antes de escribirlos, pero la idea es no recolectar una lista grande.
# Si solo necesitas los objetos a nivel raíz de 'results' tal cual, podrías simplificarla
# o usar json.dumps directamente.

def convert_bigjson_item_to_native(bj_item):
    """
    Converts a single bigjson object/array instance to its native Python equivalent.
    This is for individual items, not the entire array.
    """
    if isinstance(bj_item, BigJSONArray):
        # Recursively convert inner items for nested arrays
        return [convert_bigjson_item_to_native(item) for item in bj_item]
    elif isinstance(bj_item, BigJSONObject):
        # Recursively convert inner values for nested objects
        return {key: convert_bigjson_item_to_native(value) for key, value in bj_item.items()}
    else:
        # For basic types (str, int, float, bool, None), return as is.
        return bj_item

def extract_results_streamed(input_filepath, output_filepath, batch_size=1000):
    """
    Reads a large JSON file, extracts the content of the 'results' array
    under 'd', and saves it to a new JSON file, streaming the output
    to avoid loading the entire results array into memory.

    Args:
        input_filepath (str): Path to the input JSON file.
        output_filepath (str): Path to save the extracted results.
        batch_size (int): Not directly used for JSON streaming, but good practice
                          for conceptual understanding of chunking if needed for other formats.
                          Here, we write item by item or in small chunks.
    """
    records_processed = 0
    try:
        with open(input_filepath, 'rb') as f_in:
            data = bigjson.load(f_in)
            bigjson_results_array = data['d']['results']

            # Open the output file for writing the JSON array structure
            with open(output_filepath, 'w') as f_out:
                f_out.write('[\n') # Start of JSON array

                first_item = True
                for item in bigjson_results_array:
                    if not first_item:
                        f_out.write(',\n') # Add comma for subsequent items

                    # Convert the bigjson item to native Python object
                    # and then immediately dump it to the file.
                    native_item = convert_bigjson_item_to_native(item)
                    json.dump(native_item, f_out, indent=4) # Write one item

                    first_item = False
                    records_processed += 1

                f_out.write('\n]\n') # End of JSON array

        print(f"Successfully extracted and streamed results from '{input_filepath}' to '{output_filepath}'.")
        print(f"Total items extracted: {records_processed}")

    except FileNotFoundError:
        print(f"Error: Input JSON file not found at '{input_filepath}'")
    except KeyError as e:
        print(f"Error: Could not find expected key in JSON structure: {e}. Check if 'd' or 'results' exist.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    input_file = "bp05_3.json"
    output_file = "bp05_.json" # Cambiado el nombre para diferenciar

    # Ejemplo de uso de la nueva función
    extract_results_streamed(input_file, output_file)
